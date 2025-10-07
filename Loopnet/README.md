# Loopnet Scraper

This script is designed to scrape property data from Loopnet pages, allowing users to extract information about properties, including names, addresses, rental rates, space usage, and more. The scraper processes pages and saves the extracted data into a CSV file. It also logs errors encountered during scraping into a separate text file.

### Feautures

- Scrapes property details, including:
  - `Listing_URL`
  - `Title`
  - `Comapny_Name`
  - `Conatct_Phone`
  - `Contact_Name_1 to n*`
  - `Total_Building_Size`
  - `Avaliability`
  - `Price`
  - `Price_S/F`
- Handles pagination automatically.
- Uses concurrency for efficient scraping of multiple URLs.
- Saves scraped data in CSV format, with live updates to the CSV file during scraping.
- Logs errors to a text file for troubleshooting.
  
### Prerequisites

Ensure you have the following installed:

- Python 3.8 or higher
- Required Python libraries:
  - `pandas`
  - `requests`
  - `beautifulsoup4`
  - `selenium`

### ENV File
- Make `.env` file and set the variables
1. `INPUT_FILE` = "input_urls.txt"
2. `OUTPUT_FILE` = "loopnet_sale.csv"
3. `DRIVER_PATH` = "Add your driver path here"

### Installation

1. Clone or download this repository.
2. Navigate to the directory containing the script.
3. Install the required dependencies:

```bash
pip install -r requirements.txt
```
1. Prepare a text file (`input_urls.txt`) containing a list of Loopnet URLs to scrape, one URL per line.
2. Run the script:
```bash
python main.py
```

3. The script will:
   - Process each URL concurrently.
   - If Driver path is not Correct or not Installed then it will Installed from script Dynamically.
   - get all URL's from pagination using selenium.
   - Save the scraped data to `loopnet_sale.csv` with live updates during the scraping process.
   - Log any errors encountered to `error_log.txt`.
  
## Output

### Data File
- The scraped data is saved in `loopnet_data.csv` with the following columns:
  - `Listing_URL`
  - `Title`
  - `Comapny_Name`
  - `Conatct_Phone`
  - `Contact_Name_1`
  - `Contact_Name_2`
  - `Contact_Name_3`
  - `Contact_Name_4` 
  - `Contact_Name_5` 
  - `Contact_Name_6`
  - `Contact_Name_7`
  - `Contact_Name_8`
  - `Contact_Name_9`
  - `Contact_Name_10`
  - `Contact_Name_11`
  - `Total_Building_Size`
  - `Avaliability`
  - `Price`
  - `Price_S/F`
  
### Error Log
- Any errors encountered during the scraping process are logged in `error_log.txt`.

## Concurrency

The script uses Python's `concurrent.futures.ThreadPoolExecutor` to process multiple URL0s concurrently, significantly reducing the time required for scraping. The data is written to the CSV file as it is scraped, ensuring live updates.