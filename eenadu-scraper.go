package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	_ "github.com/mattn/go-sqlite3"
)

const (
	baseURL       = "https://www.eenadu.net"
	urlsDBName    = "urls.db"
	articleDBName = "articles.db"
	batchSize     = 100
)

var (
	once      sync.Once
	urlsDB    *sql.DB
	articleDB *sql.DB
)

type Article struct {
	URL           string
	Title         string
	DatePublished string
	Content       string
}

func initDB() error {
	var err error
	urlsDB, err = sql.Open("sqlite3", urlsDBName)
	if err != nil {
		return err
	}

	articleDB, err = sql.Open("sqlite3", articleDBName)
	if err != nil {
		return err
	}

	return initializeTables()
}

func initializeTables() error {
	createURLsTable := `
	CREATE TABLE IF NOT EXISTS urls (
		url TEXT PRIMARY KEY,
		visited BOOLEAN,
		scraped BOOLEAN
	);`
	_, err := urlsDB.Exec(createURLsTable)
	if err != nil {
		return err
	}

	createArticlesTable := `
	CREATE TABLE IF NOT EXISTS articles (
		url TEXT PRIMARY KEY,
		title TEXT,
		date_published TEXT,
		content TEXT
	);`
	_, err = articleDB.Exec(createArticlesTable)
	if err != nil {
		return err
	}

	return seedBaseURL()
}

func seedBaseURL() error {
	row := urlsDB.QueryRow("SELECT COUNT(*) FROM urls")
	var count int
	err := row.Scan(&count)
	if err != nil {
		return err
	}

	if count == 0 {
		_, err := urlsDB.Exec("INSERT INTO urls (url, visited, scraped) VALUES (?, FALSE, FALSE)", baseURL)
		if err != nil {
			return err
		}
	}
	return nil
}

func getNextURLs(batchSize int) (urls []string, err error) {
	once.Do(func() {
		if err := initDB(); err != nil {
			log.Fatalf("Failed to initialize databases: %v", err)
		}
	})

	rows, err := urlsDB.Query("SELECT url FROM urls WHERE visited = FALSE LIMIT ?", batchSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var url string
		if err := rows.Scan(&url); err != nil {
			return nil, err
		}
		urls = append(urls, url)
	}

	return urls, nil
}

func markURLsAsVisited(urls []string) error {
	once.Do(func() {
		if err := initDB(); err != nil {
			log.Fatalf("Failed to initialize databases: %v", err)
		}
	})

	tx, err := urlsDB.Begin()
	if err != nil {
		return err
	}

	for _, url := range urls {
		if _, err := tx.Exec("UPDATE urls SET visited = TRUE WHERE url = ?", url); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func insertNewURLs(urls []string) error {
	once.Do(func() {
		if err := initDB(); err != nil {
			log.Fatalf("Failed to initialize databases: %v", err)
		}
	})

	tx, err := urlsDB.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare("INSERT OR IGNORE INTO urls (url, visited, scraped) VALUES (?, FALSE, FALSE)")
	if err != nil {
		return err
	}

	for _, u := range urls {
		if _, err := stmt.Exec(u); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func insertArticle(article Article) error {
	once.Do(func() {
		if err := initDB(); err != nil {
			log.Fatalf("Failed to initialize databases: %v", err)
		}
	})

	_, err := articleDB.Exec("INSERT OR IGNORE INTO articles (url, title, date_published, content) VALUES (?, ?, ?, ?)",
		article.URL, article.Title, article.DatePublished, article.Content)
	if err != nil {
		return err
	}
	return nil
}

func extractContent(u string) (Article, *goquery.Document, error) {
	resp, err := http.Get(u)
	if err != nil {
		return Article{}, nil, err
	}
	defer resp.Body.Close()

	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return Article{}, nil, err
	}

	fullstorySelection := doc.Find("div.fullstory, section.fullstory")
	title := fullstorySelection.Find("h1").Text()
	content := fullstorySelection.Find("p").Text()
	datePublished := fullstorySelection.Find("div.pub-t").Text()

	return Article{
		URL:           u,
		Title:         title,
		DatePublished: datePublished,
		Content:       content,
	}, doc, nil
}

func extractURLs(doc *goquery.Document) []string {
	var urls []string
	doc.Find("a").Each(func(i int, s *goquery.Selection) {
		href, exists := s.Attr("href")
		if exists && strings.HasPrefix(href, baseURL) {
			urls = append(urls, href)
		}
	})
	return urls
}

func markURLAsScraped(url string) error {
	once.Do(func() {
		if err := initDB(); err != nil {
			log.Fatalf("Failed to initialize databases: %v", err)
		}
	})

	_, err := urlsDB.Exec("UPDATE urls SET scraped = TRUE WHERE url = ?", url)
	return err
}

func processURL(u string, wg *sync.WaitGroup) {
	// Removed the defer wg.Done() from here
	article, doc, err := extractContent(u)
	if err != nil {
		log.Printf("Error while requesting %s: %s\n", u, err)
		wg.Done() // Call Done here
		return
	}

	if err := insertArticle(article); err != nil {
		log.Printf("Error inserting article: %v", err)
		wg.Done() // Call Done here
		return
	}

	if err := markURLAsScraped(u); err != nil {
		log.Printf("Error marking URL as scraped: %v", err)
		wg.Done() // Call Done here
		return
	}

	newURLs := extractURLs(doc)
	if err := insertNewURLs(newURLs); err != nil {
		log.Printf("Error inserting new URLs: %v", err)
		wg.Done() // Call Done here
		return
	}
	wg.Done() // Call Done here
}

func processBaseURL() {
	article, doc, err := extractContent(baseURL)
	if err != nil {
		log.Printf("Error while requesting %s: %s\n", baseURL, err)
		return
	}

	if err := insertArticle(article); err != nil {
		log.Printf("Error inserting article: %v", err)
		return
	}

	newURLs := extractURLs(doc)
	if err := insertNewURLs(newURLs); err != nil {
		log.Printf("Error inserting new URLs: %v", err)
		return
	}
}

func main() {
	if err := initDB(); err != nil {
		log.Fatalf("Failed to initialize databases: %v", err)
		return
	}
	defer urlsDB.Close()
	defer articleDB.Close()

	processBaseURL()

	var wg sync.WaitGroup // Initialize the WaitGroup here

	for {
		currentURLs, err := getNextURLs(batchSize)
		if err != nil {
			log.Printf("Error getting next URLs: %v", err)
			continue
		}

		if len(currentURLs) == 0 {
			fmt.Println("No more URLs to process. Exiting.")
			break
		}

		if err := markURLsAsVisited(currentURLs); err != nil {
			log.Printf("Error marking URLs as visited: %v", err)
			continue
		}

		for _, u := range currentURLs {
			wg.Add(1)
			go func(url string) {
				processURL(url, &wg) // Just call the function here
			}(u)
		}
		wg.Wait()

		time.Sleep(1 * time.Second)
	}
}
