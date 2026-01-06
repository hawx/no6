// Command no6csv allows importing/exporting data to/from a no6 database from/to a csv file.
package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"

	"hawx.me/code/no6"
)

func main() {
	if len(os.Args) != 3 {
		printUsage()
		os.Exit(2)
		return
	}

	switch os.Args[1] {
	case "in":
		if err := runIn(os.Args[2]); err != nil {
			fmt.Fprintln(os.Stderr, "in error: "+err.Error())
			os.Exit(1)
			return
		}
	case "out":
		if err := runOut(os.Args[2]); err != nil {
			fmt.Fprintln(os.Stderr, "out error: "+err.Error())
			os.Exit(1)
			return
		}
	default:
		printUsage()
		os.Exit(2)
		return
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "usage: no6csv in PATH < FILE.csv\n       no6csv out PATH > FILE.csv")
}

func runIn(path string) error {
	store, err := no6.Open(path)
	if err != nil {
		return err
	}

	r := csv.NewReader(os.Stdin)
	line := 0

	for {
		record, err := r.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return err
		}

		if len(record) != 3 {
			return fmt.Errorf("csv must contain lines with 3 fields, got %d on line %d", len(record), line)
		}

		if err := store.Put(record[0], record[1], record[2]); err != nil {
			return err
		}

		line++
	}
}

func runOut(path string) error {
	store, err := no6.Open(path)
	if err != nil {
		return err
	}

	triples := store.Query()
	w := csv.NewWriter(os.Stdout)

	for _, triple := range triples {
		if err := w.Write([]string{triple.Subject, triple.Predicate, fmt.Sprint(triple.Object)}); err != nil {
			return err
		}
	}

	w.Flush()
	return w.Error()
}
