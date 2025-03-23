package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

var (
	dbUser     = os.Getenv("POSTGRES_USER")
	dbPassword = os.Getenv("POSTGRES_PASSWORD")
	dbHost     = os.Getenv("POSTGRES_HOST")
	dbPort     = os.Getenv("POSTGRES_PORT")
	dbName     = os.Getenv("POSTGRES_DB")
)

var databaseURL = fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", dbUser, dbPassword, dbHost, dbPort, dbName)

type SpotPrice struct {
	DateTime        string `json:"dateTime"`
	NormalizedPrice string `json:"normalizedPrice"`
	Spot            string `json:"spot"`
}

func queryAPI(startDate, endDate time.Time) ([]SpotPrice, error) {
	apiURL := "https://apicem.matbarofex.com.ar/api/v2/spot-prices"
	params := fmt.Sprintf("?spot=&from=%s&to=%s&page=1&pageSize=32000", startDate.Format("2006-01-02"), endDate.Format("2006-01-02"))
	resp, err := http.Get(apiURL + params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API request failed with status code %d", resp.StatusCode)
	}

	var result struct {
		Data []SpotPrice `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result.Data, nil
}

func downloadCCL() {
	currentTime := time.Now().Format("2006-01-02 15:04:05")
	fmt.Println("------------------------------------")
	fmt.Printf("Actualizando CCL...%s\n", currentTime)

	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	var lastDate sql.NullTime
	err = db.QueryRow("SELECT MAX(date) FROM ccl3").Scan(&lastDate)
	if err != nil {
		log.Fatalf("Failed to query last date: %v", err)
	}

	startDate := lastDate.Time.AddDate(0, 0, 1)
	endDate := time.Now()

	if startDate.After(endDate) || startDate.Equal(endDate) {
		fmt.Println("No hay necesidad de actualizar datos")
		return
	}

	data, err := queryAPI(startDate, endDate)
	if err != nil {
		log.Fatalf("Failed to query API: %v", err)
	}

	if len(data) == 0 {
		fmt.Println("No data to insert. The API call returned an empty response. Estamos en fin de semana o feriado?")
		return
	}

	// Filter rows with 'spot' starting with 'CCL'
	filteredData := []SpotPrice{}
	for _, row := range data {
		if strings.HasPrefix(row.Spot, "CCL") {
			filteredData = append(filteredData, row)
		}
	}

	if len(filteredData) == 0 {
		fmt.Println("No hay datos para insertar ya que no hay un 'spot' con 'CCL'")
		return
	}

	// Pivot data into a map: DateTime -> Spot -> NormalizedPrice
	pivotData := make(map[string]map[string]float64)
	for _, row := range filteredData {
		if _, exists := pivotData[row.DateTime]; !exists {
			pivotData[row.DateTime] = make(map[string]float64)
		}
		normalizedPrice, err := strconv.ParseFloat(row.NormalizedPrice, 64)
		if err != nil {
			log.Printf("Skipping row with invalid normalizedPrice: %v", err)
			continue
		}
		pivotData[row.DateTime][row.Spot] = normalizedPrice
	}

	// Combine into 'ccl' and 'ccl3'
	insertData := []struct {
		Date string
		CCL  float64
		CCL3 float64
	}{}
	for dateTime, spots := range pivotData {
		var ccl, ccl3 float64
		// CCL: Prioridad CCL > CCL3
		if val, exists := spots["CCL"]; exists {
			ccl = val
		} else if val, exists := spots["CCL3"]; exists {
			ccl = val
		}
		// CCL3: Solo el valor de CCL3 (0 si no existe)
		if val, exists := spots["CCL3"]; exists {
			ccl3 = val
		}
		// Agregar siempre, incluso si CCL o CCL3 son 0
		insertData = append(insertData, struct {
			Date string
			CCL  float64
			CCL3 float64
		}{Date: dateTime, CCL: ccl, CCL3: ccl3})
	}

	if len(insertData) == 0 {
		fmt.Println("No hay datos válidos para insertar después de combinar")
		return
	}

	// Insert data into the database
	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}

	stmt, err := tx.Prepare("INSERT INTO ccl3 (date, ccl, ccl3) VALUES ($1, $2, $3)")
	if err != nil {
		log.Fatalf("Failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	for _, row := range insertData {
		_, err := stmt.Exec(row.Date, row.CCL, row.CCL3)
		if err != nil {
			tx.Rollback()
			log.Fatalf("Failed to insert row: %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Fatalf("Failed to commit transaction: %v", err)
	}

	fmt.Printf("Inserted %d rows\n", len(insertData))
}

func main() {
	downloadCCL()
}
