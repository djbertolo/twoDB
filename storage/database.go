package storage

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// Database provides the main API for interacting with the database.
type Database struct {
	FileHandler *TextFileHandler
	Index       *BPlusTree
	Mutex       sync.RWMutex
}

// OpenDatabase initializes and opens the database.
func OpenDatabase(FilePath string) (*Database, error) {
	var FileHandler, Error = NewTextFileHandler(FilePath)
	if Error != nil {
		return nil, Error
	}

	var Index, IndexErr = NewBPlusTree(FileHandler)
	if IndexErr != nil {
		return nil, IndexErr
	}

	var DB *Database = &Database{
		FileHandler: FileHandler,
		Index:       Index,
	}
	return DB, nil
}

// Close closes the database resources.
func (db *Database) Close() error {
	return db.FileHandler.Close()
}

// Insert adds a record to the database.
func (db *Database) Insert(ID string, Data string) error {
	db.Mutex.Lock()
	defer db.Mutex.Unlock()

	// 1. Check if key already exists
	if pageID, _, _ := db.Index.Find(ID); pageID != 0 {
		return fmt.Errorf("record with ID '%s' already exists", ID)
	}

	// 2. Allocate a new page for the record
	// (A real DB would try to fit it on an existing data page)
	DataPage, err := db.FileHandler.AllocatePage()
	if err != nil {
		return err
	}
	DataPage.Header.PageType = "Data"

	// 3. Add the record to the page
	var record = &Record{Fields: []string{ID, Data}}
	EntryIndex, err := DataPage.AddRecord(record)
	if err != nil {
		return err
	}

	// 4. Write the data page to disk
	if err := db.FileHandler.WritePage(DataPage); err != nil {
		return err
	}

	// 5. Insert the key into the B+ Tree index
	return db.Index.Insert(ID, DataPage.Header.PageID, EntryIndex)
}

// Get retrieves a record by its ID.
func (db *Database) Get(ID string) (*Record, error) {
	db.Mutex.RLock()
	defer db.Mutex.RUnlock()

	// 1. Find the record's location from the index
	PageID, EntryIndex, err := db.Index.Find(ID)
	if err != nil {
		return nil, err
	}
	if PageID == 0 {
		return nil, nil // Not found
	}

	// 2. Read the data page
	DataPage, err := db.FileHandler.ReadPage(PageID)
	if err != nil {
		return nil, err
	}

	// 3. Get the record from the page
	return DataPage.GetRecord(EntryIndex)
}

// Delete removes a record by its ID.
func (db *Database) Delete(ID string) error {
	db.Mutex.Lock()
	defer db.Mutex.Unlock()

	// 1. Find the record's location from the index
	PageID, EntryIndex, err := db.Index.Find(ID)
	if err != nil {
		return err
	}
	if PageID == 0 {
		return fmt.Errorf("record with ID '%s' not found", ID)
	}

	// 2. Read the data page
	DataPage, err := db.FileHandler.ReadPage(PageID)
	if err != nil {
		return err
	}

	// 3. Delete the record from the page
	if err := DataPage.DeleteRecord(EntryIndex); err != nil {
		return err
	}

	// 4. Write the modified data page back to disk
	if err := db.FileHandler.WritePage(DataPage); err != nil {
		return err
	}

	// 5. Delete the key from the B+ Tree index
	return db.Index.Delete(ID)
}

// Update changes the data for an existing record.
func (db *Database) Update(ID string, NewData string) error {
	db.Mutex.Lock()
	defer db.Mutex.Unlock()

	// 1. Find the record's location
	PageID, EntryIndex, err := db.Index.Find(ID)
	if err != nil {
		return err
	}
	if PageID == 0 {
		return fmt.Errorf("cannot update non-existent record with ID '%s'", ID)
	}

	// 2. Read the page
	DataPage, err := db.FileHandler.ReadPage(PageID)
	if err != nil {
		return err
	}

	// 3. Get the old record to preserve its structure
	OldRecord, err := DataPage.GetRecord(EntryIndex)
	if err != nil {
		return err
	}

	// 4. Update the fields and write back
	// This simple implementation just replaces the second field.
	OldRecord.Fields[1] = NewData
	DataPage.Data["Entry-"+strconv.FormatUint(uint64(EntryIndex), 10)] = strings.Join(OldRecord.Fields, "|")

	return db.FileHandler.WritePage(DataPage)
}
