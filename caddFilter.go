/*  CADD filter

    Copyright (C) <2021> <G. Miclotte>

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package main

import (
	"bufio"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"strconv"
	"strings"
)

func main() {
	pipe, err := os.Stdin.Stat()
	if err != nil {
		panic(err)
	}

	// check piped input
	if pipe.Mode()&os.ModeNamedPipe == 0 ||
		len(os.Args) != 2 &&
			len(os.Args) != 6 &&
			len(os.Args) != 7 {
		log.Error("Usage: <program that writes CADD data to stdout> | caddFilter <input.tsv> <CHROM col> <POS col> <REF col> <ALT col> <sep>")
		return
	}

	// open filter tsv
	file, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Error(err)
		}
	}(file)

	// get filter tsv columns
	cols := [4]int{0, 1, 2, 3}
	if len(os.Args) > 2 {
		for i := range cols {
			cols[i], err = strconv.Atoi(os.Args[2+i])
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	// get filter tsv separator
	sep := "\t"
	if len(os.Args) > 6 {
		sep = os.Args[6]
	}
	log.Info("filter separator: " + sep)
	log.Info(os.Args)

	// read filter tsv
	scanner := bufio.NewScanner(file)
	variants := make(map[string]string)
	if scanner.Scan() {
		// skip the first line
		fmt.Printf("#Data header: %s\n", scanner.Text())
	}
	for scanner.Scan() {
		line := scanner.Text()
		s := strings.Split(line, sep)
		// remove "chr" from chromosome numbers
		chr := s[cols[0]]
		chr = strings.ToUpper(chr)
		chr = strings.ReplaceAll(chr, "CHR", "")
		v := strings.Join([]string{
			chr,
			s[cols[1]],
			s[cols[2]],
			s[cols[3]]},
			sep,
		)
		variants[v] = line
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// read pipe input
	reader := bufio.NewReader(os.Stdin)

	for {
		inputBytes, _, err := reader.ReadLine()
		if err != nil && err == io.EOF {
			break
		}
		input := string(inputBytes[:])
		if input[0] == '#' {
			fmt.Printf("#CADD header: %s\n", input)
			continue
		}
		// process pipe
		s := strings.Split(input, "\t") // CADD is always tab-separated
		v := strings.Join([]string{s[0], s[1], s[2], s[3]}, sep)
		scores := strings.Join([]string{s[4], s[5]}, sep)
		if _, ok := variants[v]; ok {
			fmt.Printf("%s%s%s\n", variants[v], sep, scores)
			variants[v] = ""
		}
	}

	// write all remaining variants without CADD scores
	missing := false
	for _, remaining := range variants {
		if len(remaining) > 0 {
			fmt.Printf("%s\n", remaining)
			missing = true
		}
	}
	if missing {
		log.Error("Some entries were not found.")
	}
}
