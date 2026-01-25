package segmentfile

import (
	"os"
	"sync"
)

type SegmentFileManager interface {
	Append(data []byte) (offset int64, length int64, err error)
	ReadAt(offset int64, length int64) ([]byte, error)
	Delete(offset int64) error
}

type localFileManager struct {
	file *os.File
	path string //for Delete() to find file
	mu   sync.Mutex
}

func NewSegmentFileManager(path string) (SegmentFileManager, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &localFileManager{
		file: f,
		path: path, // Initialize the path
	}, nil
}

// adds data to the end of the file,returns location
func (s *localFileManager) Append(data []byte) (int64, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	//current size to know the start offset
	stat, err := s.file.Stat()
	if err != nil {
		return 0, 0, err
	}
	offset := stat.Size()
	length := int64(len(data))

	//write the data to the end
	_, err = s.file.Write(data)
	if err != nil {
		return 0, 0, err
	}

	return offset, length, nil
}

// retrieves a part of data without reading the whole file
func (s *localFileManager) ReadAt(offset int64, length int64) ([]byte, error) {
	buf := make([]byte, length)
	_, err := s.file.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// marks segment for removal
func (s *localFileManager) Delete(offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.file.Close(); err != nil {
		return err
	}

	return os.Remove(s.path)
}
