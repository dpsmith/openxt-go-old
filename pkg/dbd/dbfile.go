package dbd

import (
	"errors"
	"os"
	"syscall"
	"github.com/tidwall/sjson"
	"github.com/tidwall/gjson"
)

type DBFile struct {
	name string
	path string
	size int
	chunksize int
	file *os.File
	fd int
	data []byte
	cache gjson.Result
}

func (dbf *DBFile) open() error {
	if dbf.file, err = os.OpenFile(dbf.path, os.O_CREATE|os.O_RDWR, 0); err != nil {
		return err
	}

	dbf.fd = int(f.Fd())

	err := syscall.Ftruncate(db.fd, int64(dbf.size))
	if err != nil {
		dbf.file.Close()
		return err
	}

	dbf.data, err = syscall.Mmap(dbf.fd, 0, dbf.size, syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		dbf.file.Close()
		return err
	}

	if !gjson.ValidBytes(dbf.data) {
		dbf.file.Close()
		return errors.New("invalid json")
	}

	dbf.cache = gjson.ParseBytes(dbf.data)
}

func (dbf *DBFile) resize(size int) error {
	syscall.Munmap(dbf.data)
	dbf.file.Close()
	dbf.size = size
	err := dbf.open()
	if err != nil {
		return err
	}
	return nil
}

func (dbf *DBFile) close() {
	syscall.Munmap(dbf.data)
	dbf.file.Close()
}

/*
 * For DBFile operations, path expected to be in dot notation
 */

/* read value of given path */
func (dbf *DBFile) read(path string) (string, error) {
	res := dbf.cache.Get(dbf.data, path)
	if !res.Exists() {
		return "", errors.New("invalid path")
	}

	if res.IsObject() || res.IsArray() {
		return "", errors.New("not a config key")
	}

	return res.String(), nil
}

/* write value to given path */
func (dbf *DBFile) write(path string, value string) error {
	opts = sjson.Options{Optimistic: true, ReplaceInPlace: true}
	res, err := sjson.SetBytesOptions(dbf.data, path, value, opts)
	if err != nil {
		return err
	}

	if &dbf.data[cap(a)-1] != &res[cap(b)-1] {
		dbf.resize(dbf.size + dbf.chunksize)
		copy(dbf.data, res)
	}

	dbf.cache = gjson.ParseBytes(dbf.data)
	return nil
}

/* dump json of path */
func (dbf *DBFile) dump(path string) (string, error) {
	if strings.Compare(path, "") == 0 {
		return dbf.cache.Raw
	}

	res := dbf.cache.Get(dbf.data, path)
	if !res.Exists() {
		return "", errors.New("invalid path")
	}

	return res.Raw, nil
}

/* inject json at path */
func (dbf *DBFile) inject(path string, value string) error {
	opts = sjson.Options{Optimistic: true, ReplaceInPlace: true}
	res, err := sjson.SetRawBytesOptions(dbf.data, path, []byte(value), opts)
	if err != nil {
		return err
	}

	if &dbf.data[cap(a)-1] != &res[cap(b)-1] {
		dbf.resize(dbf.size + dbf.chunksize)
		copy(dbf.data, res)
	}

	dbf.cache = gjson.ParseBytes(dbf.data)
	return nil
}

/* list all keys under given path */
func (dbf *DBFile) list(path string) ([]string, error) {
	res := dbf.cache.GetBytes(dbf.data, path + "*")
	if !res.Exists() {
		return nil, errors.New("invalid path")
	}

	return res.Array(), nil
}

/* remove key and all subkeys under given path */
func (dbf *DBFile) rm(path string) error {
	res, err := sjson.Delete(dbf.data, path)
	if err != nil {
		return err
	}

	if &dbf.data[cap(a)-1] != &res[cap(b)-1] {
		copy(dbf.data, res)
	}

	dbf.cache = gjson.ParseBytes(dbf.data)
	return nil
}

/* checks if given path is an JSON Object */
func (dbf *DBFile) isObject(path string) bool {
	res := dbf.cache.Get(dbf.data, path)
	return res.IsObject()
}

/* checks if given path is defined */
func (dbf *DBFile) exists(path string) bool {
	res := dbf.cache.Get(dbf.data, path)
	return res.Exists()
}
