package storage

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
)

// TextFileHandler manages the low-level reading and writing of pages to the database file.
type TextFileHandler struct {
	FilePath         string
	File             *os.File
	Mutex            sync.RWMutex
	PageSize         int
	PageCount        uint
	DeallocatedPages []uint
}

// PageHeader contains metadata for a page.
type PageHeader struct {
	PageLSN  uint64
	PageID   uint
	PageType string
}

// Page represents a single page in the database file, which can hold data, index nodes, etc.
type Page struct {
	Header PageHeader
	Data   map[string]string
}

const HeaderSection string = "# DATABASE HEADER"
const PageSection string = "# PAGE"
const DefaultPageSize int = 4096 // bytes

// NewTextFileHandler creates a new handler for the database file.
// It either creates a new file or opens an existing one.
func NewTextFileHandler(FilePath string) (*TextFileHandler, error) {
	var File *os.File
	var Error error

	if _, Error = os.Stat(FilePath); os.IsNotExist(Error) {
		File, Error = os.Create(FilePath)
		if Error != nil {
			return nil, fmt.Errorf("Failed to create database file: %w", Error)
		}

		var InitialContent string = fmt.Sprintf("%s\nPAGESIZE=%d\nENCODING=UTF-8\nVERSION=1.0\nPAGES=0\n\n", HeaderSection, DefaultPageSize)
		if _, Error = File.WriteString(InitialContent); Error != nil {
			File.Close()
			return nil, fmt.Errorf("Failed to initialize database file: %w", Error)
		}
	} else {
		File, Error = os.OpenFile(FilePath, os.O_RDWR, 0644)
		if Error != nil {
			return nil, fmt.Errorf("Failed to open database file: %w", Error)
		}
	}

	var FileHandler *TextFileHandler = &TextFileHandler{
		FilePath:         FilePath,
		File:             File,
		PageSize:         DefaultPageSize,
		PageCount:        0,
		DeallocatedPages: []uint{},
	}

	if Error = FileHandler.LoadMetadata(); Error != nil {
		File.Close()
		return nil, Error
	}

	return FileHandler, nil
}

// LoadMetadata reads the header of the database file to load configuration.
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
			break
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
			case "DEALLOCATED_PAGES":
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

// ReadPage reads a specific page by its ID from the database file.
func (self *TextFileHandler) ReadPage(PageID uint) (*Page, error) {
	self.Mutex.RLock()
	defer self.Mutex.RUnlock()

	if PageID == 0 || PageID > self.PageCount {
		return nil, fmt.Errorf("Invalid PageID: %d, PageCount: %d", PageID, self.PageCount)
	}

	self.File.Seek(0, 0)
	var Scanner *bufio.Scanner = bufio.NewScanner(self.File)
	var PageFound bool = false
	var InTargetPage bool = false

	var Page *Page = &Page{
		Header: PageHeader{PageID: PageID},
		Data:   make(map[string]string),
	}

	var PageIDMarker = fmt.Sprintf("PageID: %d", PageID)

	for Scanner.Scan() {
		var CurrentLine string = Scanner.Text()

		if strings.HasPrefix(CurrentLine, PageSection) {
			if InTargetPage {
				break // We've reached the next page
			}
			InTargetPage = false
		}

		if strings.Contains(CurrentLine, PageIDMarker) {
			InTargetPage = true
			PageFound = true
		}

		if InTargetPage && CurrentLine != "" {
			var Parts []string = strings.SplitN(CurrentLine, ": ", 2)
			if len(Parts) != 2 {
				continue
			}
			var Key, Value string = strings.TrimSpace(Parts[0]), strings.TrimSpace(Parts[1])
			switch Key {
			case "LSN":
				fmt.Sscanf(Value, "%d", &Page.Header.PageLSN)
			case "Type":
				Page.Header.PageType = Value
			case "PageID":
				// Already have it
			default:
				Page.Data[Key] = Value
			}
		}
	}

	if !PageFound {
		return nil, fmt.Errorf("Page %d not found", PageID)
	}

	return Page, Scanner.Err()
}

// WritePage writes a page's content to the database file.
func (self *TextFileHandler) WritePage(Page *Page) error {
	self.Mutex.Lock()
	defer self.Mutex.Unlock()

	Content, Error := os.ReadFile(self.FilePath)
	if Error != nil {
		return fmt.Errorf("Failed to read database file for writing: %w", Error)
	}
	var FileContent string = string(Content)

	var PageContentBuffer strings.Builder
	PageContentBuffer.WriteString(fmt.Sprintf("%s\n", PageSection))
	PageContentBuffer.WriteString(fmt.Sprintf("PageID: %d\n", Page.Header.PageID))
	PageContentBuffer.WriteString(fmt.Sprintf("LSN: %d\n", Page.Header.PageLSN))
	PageContentBuffer.WriteString(fmt.Sprintf("Type: %s\n", Page.Header.PageType))

	for Key, Value := range Page.Data {
		PageContentBuffer.WriteString(fmt.Sprintf("%s: %s\n", Key, Value))
	}
	var NewPageContent string = PageContentBuffer.String()

	var PageIDMarker = fmt.Sprintf("PageID: %d", Page.Header.PageID)
	var StartIndex = strings.Index(FileContent, PageIDMarker)

	if StartIndex != -1 {
		// Page exists, replace it
		StartIndex = strings.LastIndex(FileContent[:StartIndex], PageSection)
		var EndIndex = strings.Index(FileContent[StartIndex+len(PageSection):], PageSection)
		if EndIndex != -1 {
			EndIndex += StartIndex + len(PageSection)
			FileContent = FileContent[:StartIndex] + NewPageContent + FileContent[EndIndex:]
		} else {
			FileContent = FileContent[:StartIndex] + NewPageContent
		}
	} else {
		// Page is new, append it
		FileContent += "\n" + NewPageContent
	}

	// Update Page Count in header if necessary
	if Page.Header.PageID > self.PageCount {
		var OldPageCountString = fmt.Sprintf("PAGES=%d", self.PageCount)
		self.PageCount = Page.Header.PageID
		var NewPageCountString = fmt.Sprintf("PAGES=%d", self.PageCount)
		FileContent = strings.Replace(FileContent, OldPageCountString, NewPageCountString, 1)
	}

	if Error = self.File.Truncate(0); Error != nil {
		return fmt.Errorf("Failed to truncate database file: %w", Error)
	}
	self.File.Seek(0, 0)
	if _, Error = self.File.WriteString(FileContent); Error != nil {
		return fmt.Errorf("Failed to write to database file: %w", Error)
	}

	return nil
}

// Close flushes and closes the database file.
func (self *TextFileHandler) Close() error {
	self.Mutex.Lock()
	defer self.Mutex.Unlock()
	if self.File != nil {
		return self.File.Close()
	}
	return nil
}

// AllocatePage finds an available page ID to use for new data.
func (self *TextFileHandler) AllocatePage() (*Page, error) {
	self.Mutex.Lock()
	defer self.Mutex.Unlock()

	var PageID uint
	if len(self.DeallocatedPages) > 0 {
		PageID = self.DeallocatedPages[0]
		self.DeallocatedPages = self.DeallocatedPages[1:]
		// TODO: Update deallocated pages list in file header
	} else {
		self.PageCount++
		PageID = self.PageCount
	}

	var NewPage *Page = &Page{
		Header: PageHeader{
			PageID: PageID,
		},
		Data: make(map[string]string),
	}
	return NewPage, nil
}
