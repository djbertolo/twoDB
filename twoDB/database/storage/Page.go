package storage

import (
	"fmt"
	"strings"
)

type Record struct {
	PageIndex uint
	Fields    []string
}

func (self *Page) AddRecord(Record *Record) error {

	if self.Header.PageType != "Data" {
		return fmt.Errorf("Not a Data Page")
	}

	var PageIndex uint = 0
	var PageIndexString string
	var KeyExists bool
	if PageIndexString, KeyExists = self.Data["PageIndex"]; KeyExists {
		fmt.Sscanf(PageIndexString, "%d", &PageIndex)
	}

	PageIndex++
	self.Data["PageIndex"] = fmt.Sprintf("%d", PageIndex)

	var PageKey string = fmt.Sprintf("PageIndex%d", PageIndex)
	self.Data[PageKey] = strings.Join(Record.Fields, "|")

	return nil

}

func (self *Page) GetRecord(PageIndex uint) (*Record, error) {

	if self.Header.PageType != "Data" {
		return nil, fmt.Errorf("Not a data page")
	}

	var PageKey string = fmt.Sprintf("PageIndex%d", PageIndex)
	var RecordString string
	var RecordExists bool
	RecordString, RecordExists = self.Data[PageKey]

	if !RecordExists {
		return nil, fmt.Errorf("Record not found: PageIndex %d", PageIndex)
	}

	return &Record{
		PageIndex: PageIndex,
		Fields:    strings.Split(RecordString, "|"),
	}, nil

}

func (self *Page) GetAllRecords() ([]*Record, error) {

	if self.Header.PageType != "Data" {
		return nil, fmt.Errorf("Not a data page")
	}

	var Records []*Record

	var PageIndex uint
	var PageIndexString string
	var PageIndexExists bool
	if PageIndexString, PageIndexExists = self.Data["PageIndex"]; PageIndexExists {
		fmt.Sscanf(PageIndexString, "%d", &PageIndex)
	}

	var Index uint
	for Index = 1; Index < PageIndex; Index++ {

		var PageKey string = fmt.Sprintf("PageIndex%d", Index)
		var RecordString string
		var RecordExists bool

		if RecordString, RecordExists = self.Data[PageKey]; RecordExists {

			var RetrievedRecord *Record = &Record{
				PageIndex: Index,
				Fields:    strings.Split(RecordString, "|"),
			}

			Records = append(Records, RetrievedRecord)

		}

	}

	return Records, nil

}
