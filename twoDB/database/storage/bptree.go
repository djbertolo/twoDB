package storage

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

const BTreeOrder = 4 // A small order for demonstration purposes

// BPlusTree represents the B+ Tree structure.
type BPlusTree struct {
	RootPageID  uint
	FileHandler *TextFileHandler
}

// BTreeNode represents a node in the B+ Tree.
type BTreeNode struct {
	Page     *Page
	IsLeaf   bool
	Keys     []string
	Children []uint   // Page IDs of child nodes
	Pointers []string // For leaf nodes, "PageID:EntryIndex"
	NextLeaf uint     // For leaf nodes, Page ID of the next leaf
}

// NewBPlusTree initializes a new B+ Tree.
func NewBPlusTree(FileHandler *TextFileHandler) (*BPlusTree, error) {
	// Check if a root page already exists
	// A real implementation would store the root PageID in a metadata page (e.g., Page 1)
	RootPage, err := FileHandler.ReadPage(1)
	if err != nil { // Assuming error means it doesn't exist, create it
		RootPage, err = FileHandler.AllocatePage()
		if err != nil {
			return nil, err
		}
		RootPage.Header.PageType = "Index"
		RootPage.Data["IsLeaf"] = "true"
		RootPage.Data["Keys"] = ""
		RootPage.Data["Pointers"] = ""
		if err := FileHandler.WritePage(RootPage); err != nil {
			return nil, err
		}
	}

	return &BPlusTree{
		RootPageID:  RootPage.Header.PageID,
		FileHandler: FileHandler,
	}, nil
}

// Insert adds a key and its data pointer to the tree.
func (tree *BPlusTree) Insert(key string, pageID uint, entryIndex uint) error {
	// This is a simplified insert. A full implementation is very complex,
	// involving node splitting and parent updates.
	RootNode, err := tree.readNode(tree.RootPageID)
	if err != nil {
		return err
	}

	// For simplicity, we'll just add to the root if it's a leaf and not full.
	if RootNode.IsLeaf && len(RootNode.Keys) < BTreeOrder-1 {
		pointer := fmt.Sprintf("%d:%d", pageID, entryIndex)

		insertIndex := sort.SearchStrings(RootNode.Keys, key)

		RootNode.Keys = append(RootNode.Keys[:insertIndex], append([]string{key}, RootNode.Keys[insertIndex:]...)...)
		RootNode.Pointers = append(RootNode.Pointers[:insertIndex], append([]string{pointer}, RootNode.Pointers[insertIndex:]...)...)

		return tree.writeNode(RootNode)
	}

	// A full implementation would find the correct leaf, insert, and split if necessary,
	// propagating splits up the tree.
	fmt.Println("Warning: B+ Tree insert is simplified. Node splitting not implemented.")
	return nil
}

// Find searches for a key in the tree and returns its data location.
func (tree *BPlusTree) Find(key string) (uint, uint, error) {
	Node, err := tree.readNode(tree.RootPageID)
	if err != nil {
		return 0, 0, err
	}

	// Simplified find: only searches the root leaf node.
	if !Node.IsLeaf {
		fmt.Println("Warning: B+ Tree find is simplified. Only searching root leaf.")
		return 0, 0, fmt.Errorf("cannot find in non-leaf root (not implemented)")
	}

	for i, k := range Node.Keys {
		if k == key {
			var pageID, entryIndex uint64
			parts := strings.Split(Node.Pointers[i], ":")
			pageID, _ = strconv.ParseUint(parts[0], 10, 32)
			entryIndex, _ = strconv.ParseUint(parts[1], 10, 32)
			return uint(pageID), uint(entryIndex), nil
		}
	}

	return 0, 0, nil // Not found
}

// Delete removes a key from the tree.
func (tree *BPlusTree) Delete(key string) error {
	// A full implementation involves finding the key, removing it, and potentially
	// merging or redistributing keys/pointers in nodes.
	fmt.Println("Warning: B+ Tree delete is simplified.")
	Node, err := tree.readNode(tree.RootPageID)
	if err != nil {
		return err
	}

	if !Node.IsLeaf {
		return fmt.Errorf("cannot delete from non-leaf root (not implemented)")
	}

	foundIndex := -1
	for i, k := range Node.Keys {
		if k == key {
			foundIndex = i
			break
		}
	}

	if foundIndex != -1 {
		Node.Keys = append(Node.Keys[:foundIndex], Node.Keys[foundIndex+1:]...)
		Node.Pointers = append(Node.Pointers[:foundIndex], Node.Pointers[foundIndex+1:]...)
		return tree.writeNode(Node)
	}

	return fmt.Errorf("key not found for deletion")
}

// readNode deserializes a page into a BTreeNode.
func (tree *BPlusTree) readNode(pageID uint) (*BTreeNode, error) {
	Page, err := tree.FileHandler.ReadPage(pageID)
	if err != nil {
		return nil, err
	}
	Node := &BTreeNode{
		Page: Page,
	}
	Node.IsLeaf, _ = strconv.ParseBool(Page.Data["IsLeaf"])
	if keys, ok := Page.Data["Keys"]; ok && keys != "" {
		Node.Keys = strings.Split(keys, ",")
	}
	if pointers, ok := Page.Data["Pointers"]; ok && pointers != "" {
		Node.Pointers = strings.Split(pointers, ",")
	}
	// Add children deserialization for internal nodes here...
	return Node, nil
}

// writeNode serializes a BTreeNode back into its page and writes to disk.
func (tree *BPlusTree) writeNode(node *BTreeNode) error {
	node.Page.Data["IsLeaf"] = strconv.FormatBool(node.IsLeaf)
	node.Page.Data["Keys"] = strings.Join(node.Keys, ",")
	node.Page.Data["Pointers"] = strings.Join(node.Pointers, ",")
	// Add children serialization for internal nodes here...
	return tree.FileHandler.WritePage(node.Page)
}
