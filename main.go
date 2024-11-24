package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"strings"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/mmcdole/gofeed"
)

func main() {
	fmt.Println("Primus - journal of titles / Примус - журнал заголовков")

	feedsFileName := "feeds.tsv"

	// Check if the file exists
	if _, err := os.Stat(feedsFileName); os.IsNotExist(err) {
		// File does not exist, create it
		file, err := os.Create(feedsFileName)
		if err != nil {
			fmt.Println("Error creating file:", err)
			return
		}
		defer file.Close()

		// Write "sample" to the file
		sample := "Habr.Com\thttps://habr.com/ru/rss/all/all/\tРусскоязычный веб-сайт в формате системы тематических коллективных блогов (именуемых хабами) с элементами новостного сайта, созданный для публикации новостей, аналитических статей, мыслей, связанных с информационными технологиями, бизнесом и интернетом.\nOverClockers.Ru\thttps://overclockers.ru/rss/all.rss\tОдин из крупнейших информационных сайтов в России, посвященный компьютерам, мобильным устройствам, компьютерным играм, электромобилям и информационным технологиям в целом.\n3DNews.Ru\thttps://3dnews.ru/news/rss/\tПервое независимое российское онлайн-издание, посвящённое цифровым технологиям. 3DNews Daily Digital Digest.\n"
		_, err = file.WriteString(sample)
		if err != nil {
			fmt.Println("Error writing to file:", err)
			return
		}

		fmt.Println("File created and written successfully.")
	} else {
		fmt.Println("File already exists.")
	}

	file, err := os.Open("feeds.tsv")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	fp := gofeed.NewParser()

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, "\t")
		if len(parts) < 2 {
			fmt.Println("Invalid line format:", line)
			continue
		}
		url := parts[1]
		feed, err := fp.ParseURL(url)
		if err != nil {
			fmt.Println("Error parsing feed:", err)
			continue
		}
		println(feed.Title)
		saveToDuckDB(feed)
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
	}
}
func saveToDuckDB(feed *gofeed.Feed) {
	conn, err := sql.Open("duckdb", "primus.db")
	if err != nil {
		fmt.Println("Error connecting to DuckDB:", err)
		return
	}
	defer conn.Close()

	// Create a table if it doesn't exist
	_, err = conn.Exec(`CREATE TABLE IF NOT EXISTS rss_feeds (
        title TEXT,
        link TEXT,
        description TEXT,
        published TEXT
    )`)
	if err != nil {
		fmt.Println("Error creating table:", err)
		return
	}

	// Insert feed data
	for _, item := range feed.Items {
		_, err = conn.Exec(`INSERT INTO rss_feeds (title, link, description, published) VALUES (?, ?, ?, ?)`,
			item.Title, item.Link, item.Description, item.Published)
		if err != nil {
			fmt.Println("Error inserting data:", err)
		}
	}
}
