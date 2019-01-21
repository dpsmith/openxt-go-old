package dbd

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"github.com/tidwall/sjson"
)

type DBStore struct {
	path string
	isdir bool `json:"is_dir"`
	growsize int `json:"grow_size"`
	cachedir string `json:"-"`
	files map[string]*DBFile `json:"-"`
}

type DBConfig struct {
	stores map[string]DBStore
}

type Database struct {
	config DBConfig
	tmpdir string
	wlock sync.Mutex
}

func (s *DBStore) addFile(file *os.File) error {
	name := strings.TrimSuffix(file.Name(), "db")
	if name == "" {
		name = file.Name()
	}

	src := s.path
	if s.isdir {
		src += "/" + file.Name()
	}
	dst := s.cachedir + "/" + file.Name()

	size, err := fcopy(src, dst)
	if err != nil {
		return err
	}

	size += s.growsize

	s.files[name] = &DBFile{name: name, size: size, chunksize: s.growsize, path: dst}
	err = s.files[name].open()
	if err != nil {
		return err
	}

	return nil
}

func (s *DBStore) close() {
	for n, f := range s.files {
		f.close()

		dst := s.path
		if s.isdir {
			dst += "/" + f.name + ".db"
		}

		_, err := fcopy(f.path, dst)
		if err != nil {
			// log copy failure
		}
	}

	err := os.RemoveAll(s.cachedir)
	if err != nil {
		// log delete failure
	}
}

func (db *Database) loadDBStore(namespace string, tmpdir string) error {
	var files []string

	store := db.config.stores[namespace]
	if store.isdir {
		list, err := ioutil.ReadDir(store.path)
		if err != nil {
			return err
		}

		for _, f := range list {
			append(files, store.path + "/" + f)
		}
	} else {
		append(files, store.path)
	}

	store.cachedir = tmpdir + "/" + namespace
	err := os.Mkdir(store.cachedir, 0750)
	if err != nil {
		return err
	}

	store.files = make(map[string]*DBFile, len(files))

	for _, f := range files {
		err := store.addFile(f)
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *Database) Open(conf string) error {
	config, err := ioutil.ReadFile(conf)
	if err !=nil {
		return err
	}

	if err := json.Unmarshal(config, &db.config); err !=nil {
		return err
	}

	if db.tmpdir, err = ioutil.TempDir("", "dbd-cache"); err != nil {
		return err
	}

	havedefault := false
	for ns, _ := range db.config.stores {
		if ns == "default" {
			if !havedefault {
				havedefault = true
			} else {
				db.Close()
				return errors.New("can only have one default data store")
			}
			if ns.isdir {
				db.Close()
				return errors.New("default data store can have only one backing file")
			}
		}
		err := db.loadDBStore(ns, db.tmpdir)
	}
}

func (db *Database) Close() {
	for _, s := range db.config.stores {
		s.close()
	}

	err := os.RemoveAll(db.tmpdir)
	if err != nil {
		// log delete failure
	}
}

func (db *Database) Read(path string) (string, error) {
	if !validPath(path) {
		return "", errors.New("invalid path format")
	}

	if path == "/" {
		return "", errors.New("/ is not a config key")
	}

	n, p := splitPath(path)
	if n == "default" {
		return nil, errors.New("invalid path format")
	}

	/* will match everyting but "default" */
	if store, ok := db.config.stores[n]; ok {
		f, k := splitPath(p)
		if dbf, ok2 := store.files[f]; ok2 {
			entry := convertPath(k)
			value, err := dbf.read(entry)
		}
	} else {
		store := db.config.stores["default"]
		entry := convertPath(path)

		dbf := store.files[0]
		if dbf.exists(entry) {
			value, err = dbf.read(entry)
			if err != nil {
				return "", err
			} else {
				return value, nil
			}
		}
	}

	return "", errors.New("cannot locate config key %s", path)
}

func (db *Database) ReadBinary(path string) ([]byte, error) {
	value, err := db.Read(path)
	return []byte(value), err
}

func (db *Database) Write(path string, value string) error {
	if !validPath(path) {
		return "", errors.New("invalid path format")
	}

	if path == "/" {
		return "", errors.New("/ is not a config key")
	}

	n, p := splitPath(path)
	if n == "default" {
		return nil, errors.New("invalid path format")
	}

	/* will match everyting but "default" */
	if store, ok := db.config.stores[n]; ok {
		f, k := splitPath(p)
		if k == "" {
			return errors.New("invalid path format")
		}
		if dbf, ok2 := store.files[f]; ok2 {
			entry := convertPath(k)
			db.wlock.Lock()
			err := dbf.write(entry, value)
			db.wlock.Unlock()
			if err != nil {
				return err
			}
		}
	} else {
		store := db.config.stores["default"]
		entry := convertPath(path)

		dbf := store.files[0]
		db.wlock.Lock()
		err := dbf.write(entry, value)
		db.wlock.Unlock()
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *Database) Dump(path string) (string, error) {
	if !validPath(path) {
		return nil, errors.New("invalid path format")
	}

	if path == "/" {
		dbf := db.config.stores["default"].files[0]
		base, _ := dbf.dump("")

		for n, s := range db.config.stores {
			if n == "default" {
				continue
			}

			for i, f := range s.files {
				k := n + "." + i
				v, _ := f.dump("")
				base = sjson.SetRaw(base, k, v)
			}
		}

		return base, nil
	}

	ns, p := splitPath(path)
	if n == "default" {
		return nil, errors.New("invalid path format")
	}

	if store, ok := db.config.stores[ns]; ok {
		f, k := splitPath(p)
		/* path is within a dbfile */
		if dbf, ok2 := store.files[f]; ok2 {
			entry := convertPath(k)
			if dbf.isObject(entry) {
				values, err := dbf.dump(entry)
				if err != nil {
					return nil, err
				}
				return values, nil
			} else {
				return nil, errors.New("path has no children")
			}
		/* must be the entire store */
		} else {
			values := "{}"
			for n, c := range store.files {
				k := ns + "." + n
				v, _ := c.dump("")
				values = sjson.SetRaw(values, k, v)
			}
			return values, nil
		}
	/* path within default */
	} else {
		dbf := db.config.stores["default"].files[0]
		entry := convertPath(p)
		if dbf.isObject(entry) {
			values, err := dbf.dump(entry)
			if err != nil {
				return nil, err
			}
			return values, nil
		} else {
			return nil, errors.New("path has no children")
		}
	}

	return "", errors.New("cannot locate config key %s", path)
}

func (db *Database) Inject(path string, value string) error {
	if !validPath(path) {
		return errors.New("invalid path format")
	}

	if path == "/" {
		return errors.New("/ is not a config key")
	}

	n, p := splitPath(path)
	if n == "default" {
		return errors.New("invalid path format")
	}

	/* will match everyting but "default" */
	if store, ok := db.config.stores[n]; ok {
		f, k := splitPath(p)
		if k == "" {
			return errors.New("invalid path format")
		}
		if dbf, ok2 := store.files[f]; ok2 {
			entry := convertPath(k)
			db.wlock.Lock()
			err := dbf.inject(entry, value)
			db.wlock.Unlock()
			if err != nil {
				return err
			}
		}
	} else {
		store := db.config.stores["default"]
		entry := convertPath(path)

		dbf := store.files[0]
		db.wlock.Lock()
		err := dbf.inject(entry, value)
		db.wlock.Unlock()
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *Database) List(path string) ([]string, error) {
	var values []string

	if !validPath(path) {
		return nil, errors.New("invalid path format")
	}

	if path == "/" {
		for n, _ := range db.config.stores {
			if n == "default" {
				dbf := db.config.stores[n].files[0]
				v, err := dbf.list("")
				if err != nil {
					return nil, err
				}
				append(values, v...)
			} else {
				append(values, n)
			}
		}

		return values, nil
	}

	n, p := splitPath(path)
	if n == "default" {
		return nil, errors.New("invalid path format")
	}

	if store, ok := db.config.stores[n]; ok {
		f, k := splitPath(p)
		if dbf, ok2 := store.files[f]; ok2 {
			entry := convertPath(k)
			if dbf.isObject(entry) {
				values, err := dbf.list(entry)
				if err != nil {
					return nil, err
				}
				return values, nil
			}
		} else {
			var values bytes.Buffer
			for fname, _ := range store.files {
				values.WriteString(fname)
				values.WriteString(" ")
			}
		}
	}

	return "", errors.New("cannot locate config key %s", path)
}

func (db *Database) Rm(path string) error {
	if !validPath(path) {
		return errors.New("invalid path format")
	}

	if path == "/" {
		return errors.New("cannot remove /")
	}

	n, p := splitPath(path)
	if n == "default" {
		return errors.New("invalid path format")
	}

	/* will match everyting but "default" */
	if store, ok := db.config.stores[n]; ok {
		f, k := splitPath(p)
		if dbf, ok2 := store.files[f]; ok2 {
			entry := convertPath(k)
			db.wlock.Lock()
			err := dbf.rm(entry)
			db.wlock.Unlock()
			if err != nil {
				return err
			}
		}
	} else {
		store := db.config.stores["default"]
		entry := convertPath(path)

		dbf := store.files[0]
		db.wlock.Lock()
		err = dbf.rm(entry)
		db.wlock.Unlock()
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *Database) Exists(path string) (bool, error) {
	if !validPath(path) {
		return false, errors.New("invalid path format")
	}

	if path == "/" {
		return true, nil
	}

	n, p := splitPath(path)
	if n == "default" {
		return false, errors.New("invalid path format")
	}

	/* will match everyting but "default" */
	if store, ok := db.config.stores[n]; ok {
		f, k := splitPath(p)
		if dbf, ok2 := store.files[f]; ok2 {
			entry := convertPath(k)
			return dbf.exists(entry), nil
		}
	} else {
		store := db.config.stores["default"]
		entry := convertPath(path)

		dbf := store.files[0]
		return dbf.exists(entry), nil
	}

	return false, errors.New("cannot locate config key %s", path)
}
