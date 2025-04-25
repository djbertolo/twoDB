package storage

import (
	"fmt"
	"strings"
)

type Record struct {
	EntryIndex uint
	Fields     []string
}

func (self *Page) AddRecord(Record *Record) error {

	if self.Header.PageType != "Data" {
		return fmt.Errorf("Not a Data Page")
	}

	var EntryIndex uint = 0
	var EntryIndexString string
	var KeyExists bool
	if EntryIndexString, KeyExists = self.Data["EntryIndex"]; KeyExists {
		fmt.Sscanf(EntryIndexString, "%d", &EntryIndex)
	}

	EntryIndex++
	self.Data["EntryIndex"] = fmt.Sprintf("%d", EntryIndex)

	var EntryKey string = fmt.Sprintf("EntryIndex%d", EntryIndex)
	self.Data[EntryKey] = strings.Join(Record.Fields, "|")

	return nil

}

func (self *Page) GetRecord(EntryIndex uint) (*Record, error) {

	if self.Header.PageType != "Data" {
		return nil, fmt.Errorf("Not a data page")
	}

	var EntryKey string = fmt.Sprintf("EntryIndex%d", EntryIndex)
	var RecordString string
	var RecordExists bool
	RecordString, RecordExists = self.Data[EntryKey]

	if !RecordExists {
		return nil, fmt.Errorf("Record not found: PageIndex %d", EntryIndex)
	}

	return &Record{
		EntryIndex: EntryIndex,
		Fields:     strings.Split(RecordString, "|"),
	}, nil

}

func (self *Page) GetAllRecords() ([]*Record, error) {

	if self.Header.PageType != "Data" {
		return nil, fmt.Errorf("Not a data page")
	}

	var Records []*Record

	var EntryIndex uint
	var EntryIndexString string
	var EntryIndexExists bool
	if EntryIndexString, EntryIndexExists = self.Data["EntryIndex"]; EntryIndexExists {
		fmt.Sscanf(EntryIndexString, "%d", &EntryIndex)
	}

	var Index uint
	for Index = 1; Index < EntryIndex; Index++ {

		var EntryKey string = fmt.Sprintf("EntryIndex%d", Index)
		var RecordString string
		var RecordExists bool

		if RecordString, RecordExists = self.Data[EntryKey]; RecordExists {

			var RetrievedRecord *Record = &Record{
				EntryIndex: Index,
				Fields:     strings.Split(RecordString, "|"),
			}

			Records = append(Records, RetrievedRecord)

		}

	}

	return Records, nil

}
