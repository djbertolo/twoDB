package storage

import (
	"fmt"
	"strings"
)

// Record represents a single row or entry in a data page.
type Record struct {
	EntryIndex uint
	Fields     []string
}

// AddRecord adds a new record to a data page.
func (self *Page) AddRecord(Record *Record) (uint, error) {
	if self.Header.PageType != "Data" {
		return 0, fmt.Errorf("Not a Data Page")
	}

	var EntryIndex uint = 0
	if EntryIndexString, KeyExists := self.Data["EntryIndex"]; KeyExists {
		fmt.Sscanf(EntryIndexString, "%d", &EntryIndex)
	}

	EntryIndex++
	self.Data["EntryIndex"] = fmt.Sprintf("%d", EntryIndex)

	var EntryKey string = fmt.Sprintf("Entry-%d", EntryIndex)
	self.Data[EntryKey] = strings.Join(Record.Fields, "|")

	Record.EntryIndex = EntryIndex
	return EntryIndex, nil
}

// GetRecord retrieves a record by its index from a data page.
func (self *Page) GetRecord(EntryIndex uint) (*Record, error) {
	if self.Header.PageType != "Data" {
		return nil, fmt.Errorf("Not a data page")
	}

	var EntryKey string = fmt.Sprintf("Entry-%d", EntryIndex)
	var RecordString string
	var RecordExists bool
	RecordString, RecordExists = self.Data[EntryKey]

	if !RecordExists {
		return nil, fmt.Errorf("Record not found on page: EntryIndex %d", EntryIndex)
	}

	return &Record{
		EntryIndex: EntryIndex,
		Fields:     strings.Split(RecordString, "|"),
	}, nil
}

// DeleteRecord removes a record from a page.
func (self *Page) DeleteRecord(EntryIndex uint) error {
	if self.Header.PageType != "Data" {
		return fmt.Errorf("Not a data page")
	}
	var EntryKey string = fmt.Sprintf("Entry-%d", EntryIndex)
	if _, exists := self.Data[EntryKey]; !exists {
		return fmt.Errorf("Record to delete not found on page: EntryIndex %d", EntryIndex)
	}
	delete(self.Data, EntryKey)
	return nil
}
