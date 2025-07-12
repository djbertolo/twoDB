package main

import (
	"fmt"
	"log"
	"twoDB/storage"
)

func main() {
	// 1. Open the database. This creates a file named "mydatabase.db" if it doesn't exist.
	db, err := storage.OpenDatabase("mydatabase.db")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close() // 6. Ensure the database is closed when the program exits.

	fmt.Println("Database opened successfully.")

	// 2. Insert data
	fmt.Println("\n--- Inserting Records ---")
	err = db.Insert("user:1", "John Doe")
	if err != nil {
		log.Printf("Insert failed: %v", err)
	} else {
		fmt.Println("Inserted 'user:1'")
	}

	err = db.Insert("user:2", "Jane Smith")
	if err != nil {
		log.Printf("Insert failed: %v", err)
	} else {
		fmt.Println("Inserted 'user:2'")
	}

	// Trying to insert a duplicate key
	err = db.Insert("user:1", "John Doe Again")
	if err != nil {
		fmt.Printf("As expected, failed to insert duplicate key: %v\n", err)
	}

	// 3. Get data
	fmt.Println("\n--- Retrieving Records ---")
	record, err := db.Get("user:1")
	if err != nil {
		log.Printf("Get failed: %v", err)
	} else if record != nil {
		fmt.Printf("Retrieved 'user:1': ID=%s, Data=%s\n", record.Fields[0], record.Fields[1])
	} else {
		fmt.Println("Record 'user:1' not found.")
	}

	// 4. Update data
	fmt.Println("\n--- Updating a Record ---")
	err = db.Update("user:2", "Jane Doe")
	if err != nil {
		log.Printf("Update failed: %v", err)
	} else {
		fmt.Println("Updated 'user:2'")
	}

	// Verify the update
	record, err = db.Get("user:2")
	if err != nil {
		log.Printf("Get failed: %v", err)
	} else if record != nil {
		fmt.Printf("Retrieved updated 'user:2': ID=%s, Data=%s\n", record.Fields[0], record.Fields[1])
	}

	// 5. Delete data
	fmt.Println("\n--- Deleting a Record ---")
	err = db.Delete("user:1")
	if err != nil {
		log.Printf("Delete failed: %v", err)
	} else {
		fmt.Println("Deleted 'user:1'")
	}

	// Verify the deletion
	record, err = db.Get("user:1")
	if err == nil && record == nil {
		fmt.Println("As expected, record 'user:1' not found after deletion.")
	}
}
