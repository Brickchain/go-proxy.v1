package cache

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	logger "github.com/Brickchain/go-logger.v1"
)

type File struct {
	data     map[string]*Value
	mu       sync.Mutex
	dir      string
	interval time.Duration
	changes  uint64
	stop     bool
}

func NewFile(dir string, interval time.Duration) (*File, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	f := &File{
		data:     make(map[string]*Value),
		mu:       sync.Mutex{},
		dir:      dir,
		interval: interval,
		changes:  0,
		stop:     false,
	}

	if err := f.load(); err != nil {
		return nil, err
	}

	go f.save()
	go f.reaper()

	return f, nil
}

func (f *File) Stop() {
	f.stop = true
}

func (f *File) listCacheFiles() ([]string, error) {
	files, err := ioutil.ReadDir(f.dir)
	if err != nil {
		return nil, err
	}

	cacheFiles := make([]string, 0)
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".cache") {
			cacheFiles = append(cacheFiles, file.Name())
		}
	}

	return cacheFiles, nil
}

func (f *File) load() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	files, err := f.listCacheFiles()
	if err != nil {
		return err
	}

	if len(files) > 0 {
		sort.Strings(files)

		for i := len(files) - 1; i > -1; i-- {
			file, err := os.OpenFile(fmt.Sprintf("%s/%s", f.dir, files[i]), os.O_RDONLY, 0644)
			if err != nil {
				logger.Error(err)
				continue
			}

			zr, err := gzip.NewReader(file)
			if err != nil {
				logger.Error(err)
				continue
			}

			unziped := bytes.NewBuffer([]byte{})
			if _, err = io.Copy(unziped, zr); err != nil {
				logger.Error(err)
				continue
			}

			err = json.Unmarshal(unziped.Bytes(), &f.data)
			if err == nil {
				return nil
			}
		}
	}

	return nil
}

func (f *File) save() {
	for {
		time.Sleep(f.interval)

		if f.stop {
			return
		}

		if f.changes > 0 {
			f.mu.Lock()

			content, err := json.Marshal(f.data)
			if err == nil {
				filename := fmt.Sprintf("%d.cache", time.Now().UnixNano())

				var buf bytes.Buffer
				zw := gzip.NewWriter(&buf)
				zw.ModTime = time.Now()

				_, err = zw.Write(content)
				if err != nil {
					logger.Error(err)
					continue
				}
				if err = zw.Flush(); err != nil {
					logger.Error(err)
					continue
				}
				if err = zw.Close(); err != nil {
					logger.Error(err)
					continue
				}

				snapshot, err := os.OpenFile(fmt.Sprintf("%s/%s", f.dir, filename), os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					logger.Error(err)
					continue
				}

				if _, err = io.Copy(snapshot, &buf); err != nil {
					logger.Error(err)
					continue
				}

				// clean up old snapshots
				files, _ := f.listCacheFiles()
				if len(files) > 1 {
					for _, file := range files {
						if file != filename {
							os.Remove(fmt.Sprintf("%s/%s", f.dir, file))
						}
					}
				}
				f.changes = 0
			}

			f.mu.Unlock()
		}
	}
}

func (f *File) reaper() {
	for {
		if f.stop {
			return
		}

		f.mu.Lock()
		for key, val := range f.data {
			if val.Expiry.Before(time.Now()) {
				delete(f.data, key)
				f.changes++
			}
		}
		f.mu.Unlock()
		time.Sleep(time.Second * 10)
	}
}

func (f *File) Get(key string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	val, ok := f.data[key]
	if !ok {
		return nil, fmt.Errorf("Key not found")
	}

	if val.Expiry.Before(time.Now()) {
		return nil, fmt.Errorf("Key not found")
	}

	return val.Data, nil
}

func (f *File) Set(key string, data []byte, ttl time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	val := &Value{
		Data:   data,
		Expiry: time.Now().Add(ttl),
	}

	f.data[key] = val
	f.changes++

	return nil
}

func (f *File) GetTTL(key string) (time.Duration, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	val, ok := f.data[key]
	if !ok {
		return time.Duration(0), fmt.Errorf("Key not found")
	}

	return val.Expiry.Sub(time.Now()), nil
}

func (f *File) Extend(key string, ttl time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	val, ok := f.data[key]
	if !ok {
		return fmt.Errorf("Key not found")
	}

	val.Expiry = time.Now().Add(ttl)

	f.data[key] = val
	f.changes++

	return nil
}
