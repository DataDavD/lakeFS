package local

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/uuid"
	"github.com/treeverse/lakefs/block"
	"hash"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type Adapter struct {
	path               string
	ctx                context.Context
	uploadIdTranslator block.UploadIdTranslator
}

func (s *Adapter) InjectSimulationId(u block.UploadIdTranslator) {
	s.uploadIdTranslator = u
}

func (l *Adapter) WithContext(ctx context.Context) block.Adapter {
	return &Adapter{
		path:               l.path,
		ctx:                ctx,
		uploadIdTranslator: l.uploadIdTranslator,
	}
}

func NewAdapter(path string) (block.Adapter, error) {
	stt, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !stt.IsDir() {
		return nil, fmt.Errorf("path provided is not a valid directory")
	}
	if !isDirectoryWritable(path) {
		return nil, fmt.Errorf("path provided is not writable")
	}
	return &Adapter{path: path, ctx: context.Background(), uploadIdTranslator: &block.DummyTranslator{}}, nil
}

func (l *Adapter) getPath(identifier string) string {
	return path.Join(l.path, identifier)
}

func (l *Adapter) Put(_ string, identifier string, _ int64, reader io.Reader) error {
	path := l.getPath(identifier)
	f, err := os.Create(path)
	defer f.Close()
	_, err = io.Copy(f, reader)
	if err != nil {
		return err
	}
	return nil
}

func (l *Adapter) Remove(_ string, identifier string) error {
	path := l.getPath(identifier)
	err := os.Remove(path)
	return err
}

func (l *Adapter) Get(_ string, identifier string) (reader io.ReadCloser, err error) {
	path := l.getPath(identifier)
	f, err := os.OpenFile(path, os.O_RDONLY, 0755)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (l *Adapter) GetRange(_ string, identifier string, start int64, end int64) (io.ReadCloser, error) {
	path := l.getPath(identifier)
	f, err := os.OpenFile(path, os.O_RDONLY, 0755)
	if err != nil {
		return nil, err
	}
	_, err = f.Seek(start, 0)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (l *Adapter) GetAdapterType() string {
	return "local"
}

func isDirectoryWritable(pth string) bool {
	// test ability to write to directory.
	// as there is no simple way to test this in windows, I prefer the "brute force" method
	// of creating s dummy file. will work in any OS.
	// speed is not an issue, as this will be activated very few times during startup

	fileName := path.Join(pth, "dummy.tmp")
	os.Remove(fileName)
	file, err := os.Create(fileName)
	if err == nil {
		file.Close()
		os.Remove(fileName)

		return true
	} else {
		return false
	}
}

func (l *Adapter) CreateMultiPartUpload(repo string, identifier string, r *http.Request) (string, error) {
	if strings.Contains(identifier, "/") {
		fullPath := l.getPath(identifier)
		fullDir := path.Dir(fullPath)
		err := os.MkdirAll(fullDir, 0755)
		if err != nil {
			fmt.Errorf("failed to create directory: " + fullDir)
			return "", nil
		}

	}
	x := ([16]byte(uuid.New()))
	uploadId := hex.EncodeToString(x[:])
	uploadId = l.uploadIdTranslator.SetUploadId(uploadId)
	return uploadId, nil
}

func (l *Adapter) UploadPart(repo string, identifier string, sizeBytes int64, reader io.Reader, uploadId string, partNumber int64) (string, error) {
	md5Read := newMd5Reader(reader)
	fName := uploadId + fmt.Sprintf("-%05d", (partNumber))
	err := l.Put("", fName, -1, md5Read)
	ETag := "\"" + hex.EncodeToString(md5Read.md5.Sum(nil)) + "\""
	return ETag, err
}
func (l *Adapter) AbortMultiPartUpload(repo string, identifier string, uploadId string) error {
	files, err := l.getPartFiles(uploadId)
	if err != nil {
		return err
	}
	l.removePartFiles(files)
	return nil
}
func (l *Adapter) CompleteMultiPartUpload(repo string, identifier string, uploadId string, XMLmultiPartComplete []byte) (*string, int64, error) {
	var MultipartList struct{ Parts []*s3.CompletedPart }
	//uploadId = s.uploadIdTranslator.TranslateUploadId(uploadId)
	err := xml.Unmarshal([]byte(XMLmultiPartComplete), &MultipartList)
	if err != nil {
		fmt.Errorf("failed parsing received XML: " + string(XMLmultiPartComplete))
		return nil, 0, err
	}
	ETag := computeETag(MultipartList.Parts) + "-" + strconv.Itoa(len(MultipartList.Parts))
	partFiles, err := l.getPartFiles(uploadId)
	if err != nil {
		fmt.Errorf("did not find part files for: " + uploadId)
		return nil, -1, err
	}
	size, err := l.unitePartFiles(identifier, partFiles)
	if err != nil {
		fmt.Errorf("faile multipart upload file unification: " + uploadId)
		return nil, -1, err
	}
	l.removePartFiles(partFiles)
	return &ETag, size, nil

}

func computeETag(Parts []*s3.CompletedPart) string {
	var ETagHex []string
	for _, p := range Parts {
		e := *p.ETag
		if strings.HasPrefix(e, "\"") && strings.HasSuffix(e, "\"") {
			e = e[1 : len(e)-1]
		}
		ETagHex = append(ETagHex, e)
	}
	s := strings.Join(ETagHex, "")
	b, _ := hex.DecodeString(s)
	md := md5.New()
	csm := hex.EncodeToString(md.Sum(b))
	return csm
}

func (l *Adapter) unitePartFiles(identifier string, files []string) (int64, error) {
	path := l.getPath(identifier)
	unitedFile, err := os.Create(path)
	defer unitedFile.Close()
	if err != nil {
		fmt.Errorf("failed creating united multipart file : " + path)
		return 0, err
	}
	var readers = []io.Reader{}
	for _, name := range files {
		f, err := os.Open(name)
		if err != nil {
			fmt.Errorf("failed opening file : " + name)
			return 0, err
		}
		readers = append(readers, f)
		defer f.Close()
	}
	unitedReader := io.MultiReader(readers...)
	size, err := io.Copy(unitedFile, unitedReader)
	return size, err
}
func (l *Adapter) removePartFiles(files []string) {
	for _, name := range files {
		err := os.Remove(name)
		if err != nil {
			fmt.Errorf("failed removing file : " + name)
		}
	}
}

func (l *Adapter) getPartFiles(uploadId string) ([]string, error) {
	path := l.getPath(uploadId) + "-*"
	names, err := filepath.Glob(path)
	if err != nil {
		fmt.Errorf("failed Globe on: " + path)
		return nil, err
	}
	sort.Strings(names)
	return names, err
}

type md5Reader struct {
	md5            hash.Hash
	originalReader io.Reader
	copiedSize     int64
}

func (s *md5Reader) Read(p []byte) (int, error) {
	len, err := s.originalReader.Read(p)
	if len > 0 {
		s.md5.Write(p[0:len])
		s.copiedSize += int64(len)
	}
	return len, err
}

func newMd5Reader(body io.Reader) (s *md5Reader) {
	s = new(md5Reader)
	s.md5 = md5.New()
	s.originalReader = body
	return
}
