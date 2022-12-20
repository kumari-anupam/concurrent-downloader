package main

import (
	"fmt"
	"github.com/anupam111/concurrent-downloader/internal/download"
	"log"
)

func main() {
	fileUrls := []string{
	"https://www.eurofound.europa.eu/sites/default/files/ef_publication/field_ef_document/ef1710en.pdf",                                                                                                                                                  //research report 5mb
	"https://www.stats.govt.nz/assets/Uploads/Annual-enterprise-survey/Annual-enterprise-survey-2021-financial-year-provisional/Download-data/annual-enterprise-survey-2021-financial-year-provisional-csv.csv",                                          //6.3 MB
	"https://www.stats.govt.nz/assets/Uploads/New-Zealand-business-demography-statistics/New-Zealand-business-demography-statistics-At-February-2021/Download-data/Geographic-units-by-industry-and-statistical-area-2000-2021-descending-order-CSV.zip", // GEO Units (23 MB)
	"https://www.stats.govt.nz/assets/Uploads/International-trade/International-trade-June-2022-quarter/Download-data/overseas-trade-indexes-June-2022-quarter-provisional-csv.csv",                                                                      //Overseas trade indexes, 21 MB
	}

	downloadDir := "C:\\GIT\\personal-project\\downloaddir"  // It needs to be changed accordingly.

	opts := download.DownloadOptions{
		DownloadDir:  downloadDir,
		NumConcParts: 2,
		MaxLimitConcurrency: 5,
	}

	downloader := download.NewDownloader(opts)
	files, err := downloader.Download(fileUrls...)
	if err != nil {
		log.Fatalln(err)
		return
	}
	fmt.Println(files)
}
