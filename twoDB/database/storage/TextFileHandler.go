package storage

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
)

type TextFileHandler struct {
	FilePath         string
	File             *os.File
	Mutext           sync.RWMutex
	PageSize         int
	PageCount        uint
	DeallocatedPages []uint
}

type PageHeader struct {
	PageLSN  uint64
	PageID   uint
	PageType string
}

type Page struct {
	Header PageHeader
	Data   map[string]string
}

const HeaderSection string = "# DATABASE HEADER"
const PageSection string = "# PAGE"
const DefaultPageSize int = 4096 // bytes

func NewTextFileHandler(FilePath string) (*TextFileHandler, error) {

	var File *os.File
	var Error error

	// Check for file
	if _, Error := os.Stat(FilePath); os.IsNotExist(Error) {

		// Create new file
		File, Error = os.Create(FilePath)
		if Error != nil {
			return nil, fmt.Errorf("Failed to create database file: %w", Error)
		}

		// Initialize new file content
		InitalContent := fmt.Sprintf("%s\nPAGESIZE=%d\nENCODING=UTF-8\nVERSION=1.0\nPAGES=1\n\n", HeaderSection, DefaultPageSize)
		if _, Error := File.WriteString(InitalContent); Error != nil {
			File.Close()
			return nil, fmt.Errorf("Failed to intialize database file: %w", Error)
		}

	} else {

		// Open existing file
		File, Error = os.OpenFile(FilePath, os.O_RDWR, 0644)
		if Error != nil {
			return nil, fmt.Errorf("Failed to open database file: %w", Error)
		}

	}

	var FileHandler *TextFileHandler = &TextFileHandler{
		FilePath:         FilePath,
		File:             File,
		PageSize:         DefaultPageSize,
		PageCount:        1,
		DeallocatedPages: []uint{},
	}

	// Load existing metadata
	if Error = FileHandler.LoadMetadata(); Error != nil {
		File.Close()
		return nil, Error
	}

	return FileHandler, nil

}

func (self *TextFileHandler) LoadMetadata() error {

	self.File.Seek(0, 0)
	var Scanner *bufio.Scanner = bufio.NewScanner(self.File)

	var InHeader bool = false
	for Scanner.Scan() {

		var CurrentLine string = Scanner.Text()

		if CurrentLine == HeaderSection {
			InHeader = true
			continue
		} else if strings.HasPrefix(CurrentLine, PageSection) {
			InHeader = false
			continue
		}

		if InHeader && CurrentLine != "" {

			var Metadata []string = strings.SplitN(CurrentLine, "=", 2)
			if len(Metadata) != 2 {
				continue
			}

			var Key, Value string = strings.TrimSpace(Metadata[0]), strings.TrimSpace(Metadata[1])
			switch Key {
			case "PAGESIZE":
				fmt.Sscanf(Value, "%d", &self.PageSize)
			case "PAGES":
				fmt.Sscanf(Value, "%d", &self.PageCount)
			case "FreePages":
				// Parse comma-separated list of free pages
				for _, Page := range strings.Split(Value, ",") {

					var PageID uint
					fmt.Sscanf(Page, "%d", &PageID)
					self.DeallocatedPages = append(self.DeallocatedPages, PageID)

				}
			}

		}

	}

	return Scanner.Err()

}

func (self *TextFileHandler) ReadPage(PageID uint) (*Page, error) {

	self.Mutext.RLock()
	defer self.Mutext.RUnlock()

	if PageID <= 0 || PageID > self.PageCount {
		return nil, fmt.Errorf("Invalid PageID: %d", PageID)
	}

	self.File.Seek(0, 0)
	var Scanner *bufio.Scanner = bufio.NewScanner(self.File)

	// Find the page section
	var InPage bool = false

	var PageHeader *PageHeader = &PageHeader{
		PageID: PageID,
	}

	var Page *Page = &Page{
		Header: *PageHeader,
		Data:   make(map[string]string),
	}

	for Scanner.Scan() {

		var CurrentLine string = Scanner.Text()

		if strings.HasPrefix(CurrentLine, PageSection) {

			InPage = true
			continue

		} else if InPage && strings.HasPrefix(CurrentLine, PageSection) {
			// Next Page found, end of current Page
			break
		}

		if InPage && CurrentLine != "" && !strings.HasPrefix(CurrentLine, PageSection) {

			var Metadata []string = strings.SplitN(CurrentLine, ":", 2)
			if len(Metadata) != 2 {
				continue
			}

			var Key, Value string = strings.TrimSpace(Metadata[0]), strings.TrimSpace(Metadata[1])

			switch Key {
			case "LSN":

				fmt.Sscanf(Value, "%d", &Page.Header.PageLSN)

			case "Type":

				switch Value {
				case "Meta":

					Page.Header.PageType = "Metadata"

				case "Schema":

					Page.Header.PageType = "Schema"

				case "Data":

					Page.Header.PageType = "Data"

				case "Index":

					Page.Header.PageType = "Index"

				}

			default:
				Page.Data[Key] = Value
			}

		}

	}

	if !InPage {
		return nil, fmt.Errorf("Page %d not found", PageID)
	}

	return Page, Scanner.Err()

}

func (self *TextFileHandler) WritePage(Page *Page) error {

	self.Mutext.Lock()
	defer self.Mutext.Unlock()

	self.File.Seek(0, 0)

	var Content []byte
	var Error error
	Content, Error = os.ReadFile(self.FilePath)
	if Error != nil {
		return fmt.Errorf("Failed to read database file: %w", Error)
	}

	var FileContent string = string(Content)

	var PageHeader string = fmt.Sprintf("# PAGE \nPageID: %d\nLSN: %dType: %s\n", Page.Header.PageID, Page.Header.PageLSN, Page.Header.PageType)
	var PageContent string = PageHeader

	for Key, Value := range Page.Data {
		PageContent += fmt.Sprintf("%s: %s\n", Key, Value)
	}

	if strings.Contains(FileContent, PageHeader) {

		var StartIndex int = strings.Index(FileContent, PageHeader)
		var EndIndex int = StartIndex

		var NextPageIndex = strings.Index(FileContent[StartIndex+1:], "# PAGE")
		if NextPageIndex != -1 {
			EndIndex = StartIndex + 1 + NextPageIndex
		} else {
			EndIndex = len(FileContent)
		}

		FileContent = FileContent[:StartIndex] + PageContent + FileContent[EndIndex:]

	} else {

		FileContent += "\n" + PageContent

		if Page.Header.PageID > self.PageCount {

			self.PageCount = Page.Header.PageID

			var PagesCount string = fmt.Sprintf("PAGES=%d", self.PageCount)
			var StringToReplace string = fmt.Sprintf("PAGES=%d", self.PageCount-1)
			FileContent = strings.Replace(FileContent, StringToReplace, PagesCount, 1)

		}

	}

	self.File.Seek(0, 0)

	if Error = self.File.Truncate(0); Error != nil {
		return fmt.Errorf("Failed to truncate database file: %w", Error)
	}

	if _, Error = self.File.WriteString(FileContent); Error != nil {
		return fmt.Errorf("Failed to write to database file: %w", Error)
	}

	return nil

}

func (self *TextFileHandler) Close() error {

	self.Mutext.Lock()
	defer self.Mutext.Unlock()

	if self.File != nil {
		return self.File.Close()
	}

	return nil

}

func (self *TextFileHandler) AllocatePage() (uint, error) {

	self.Mutext.Lock()
	self.Mutext.Unlock()

	if len(self.DeallocatedPages) > 0 {

		var DeallocatedPageID uint = self.DeallocatedPages[0]
		self.DeallocatedPages = self.DeallocatedPages[1:]

		return DeallocatedPageID, nil

	}

	self.PageCount++

	self.File.Seek(0, 0)

	var Content []byte
	var Error error
	Content, Error = os.ReadFile(self.FilePath)
	if Error != nil {
		return 0, fmt.Errorf("Failed to read database file: %w", Error)
	}

	var FileContent string = string(Content)

	var PageCountString string = fmt.Sprintf("PAGES=%d", self.PageCount)
	var StringToReplace string = fmt.Sprintf("PAGES=%d", self.PageCount-1)
	FileContent = strings.Replace(FileContent, StringToReplace, PageCountString, 1)

	self.File.Seek(0, 0)
	if Error = self.File.Truncate(0); Error != nil {
		return 0, fmt.Errorf("Failed to truncate database file: %w", Error)
	}

	if _, Error = self.File.WriteString(FileContent); Error != nil {
		return 0, fmt.Errorf("Failed to update database file: %w", Error)
	}

	return self.PageCount, nil

}
