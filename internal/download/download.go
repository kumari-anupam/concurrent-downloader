package download

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// DownloadClient is a simple HTTP Downloader that supports
// concurrent downloading of files.
type DownloadClient interface {
	// Download downloads the given urls into the configured downloadDir using
	// DownloadOptions.NumConcPartsand DownloadOptions.NumConcFiles
	// appropriately and returns paths to the locally downloaded files or error.
	Download(fileUrls ...string) (downloadPaths []string, err error)
}

type DownloadOptions struct {
	DownloadDir string
	// NumConcParts represents max number of go-routines used to download diff parts
	// of a large file simultaneously.
	// Only use when file size > 10MB.
	NumConcParts int
	// MaxLimitConcurrency represents number of max goroutine.
	MaxLimitConcurrency int
}

// Downloader ...
type Downloader struct {
	err             error
	downloadOptions DownloadOptions
	downloadPaths []string
}

// NewDownloader ...
func NewDownloader(opts DownloadOptions) Downloader {
	return Downloader{downloadOptions: opts}
}

func (d *Downloader) Download(fileUrls ...string) (downloadPaths []string, err error) {
	wg := &sync.WaitGroup{}
	waitChan := make(chan struct{}, d.downloadOptions.MaxLimitConcurrency)
	for _, fileUri := range fileUrls {
		subStringsSlice := strings.Split(fileUri, "/")
		fileName := subStringsSlice[len(subStringsSlice)-1]
		fileSize, err := checkFileSizeWithHeaderContentLength(fileUri)
		if err != nil {
			return downloadPaths, fmt.Errorf("error while checking the size of the file")
		}
		wg.Add(1)
		go d.downloadLargeFile(wg, fileUri, fileName, int(fileSize), waitChan)
		if d.err != nil {
			return downloadPaths, fmt.Errorf("error while processing based on contentlength, %v", err)
		}
	}
	wg.Wait()
	return d.downloadPaths, nil
}

//createOutputFile ...
func createOutputFile(path string) (*os.File,error) {
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		return nil, fmt.Errorf("File already exists : %w", path)
	}

	outFile, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("Error while creating file : %v", err)
	}

	return outFile, nil
}

// downloadLargeFile downloads file > 10MB concurrently using goroutines.
func (d *Downloader) downloadLargeFile(wg *sync.WaitGroup, url, fileName string, contentLength int, waitchan chan struct{}) {
	if wg != nil {
		defer wg.Done()
	}
	outputFilePath := filepath.Join(d.downloadOptions.DownloadDir, filepath.Base(fileName))
	outFile, err := createOutputFile(outputFilePath)
	if err != nil{
		d.err = fmt.Errorf("%w", err)
		return
	}

	fileChunks := map[int]*os.File{}

	//Close the output file after everything is done
	defer outFile.Close()

	fmt.Printf("total size of file \"%s\" is %d\n", fileName, contentLength)
	lastByteLeft := 0
	numConcParts := 0
	totalBytesPerGoroutine := 0

	if contentLength <= (1024 * 1024 * 10){
		numConcParts = 1
		lastByteLeft = contentLength
	}else{
		numConcParts = d.downloadOptions.NumConcParts
		totalBytesPerGoroutine = contentLength/d.downloadOptions.NumConcParts
		lastByteLeft = contentLength % (d.downloadOptions.NumConcParts)
	}

	wg1 := &sync.WaitGroup{}
	index := 0

	for i := 0; i < numConcParts; i++ {
		min := (totalBytesPerGoroutine) * i
		max := (totalBytesPerGoroutine) * (i + 1) - 1

		if (i == (numConcParts - 1)) && (lastByteLeft > 0) {
			max = max + lastByteLeft
		}

		f, err := os.CreateTemp("", fileName+".*.part")
		if err != nil {
			d.err = fmt.Errorf("error while creating the temporary file in the same directory: %w", err)
			return
		}
		defer f.Close()
		defer os.Remove(f.Name())                        // removing temp files.
		fileChunks[index] = f //storing temp file created by goroutine to store chunk of file.
		waitchan <- struct{}{}
		fmt.Printf("goroutine downloading file %s part for range %d-%d\n",fileName,min,max)
		wg1.Add(1)
		go d.downloadFileForRange(wg1, url, strconv.Itoa(min)+"-"+strconv.Itoa(max), f, waitchan)
		index++
	}

	if d.err != nil {
		d.err = fmt.Errorf("error while downloading file for range using goroutine, error: %w", d.err)
		return
	}

	wg1.Wait()

	err =  d.combineChunks(fileChunks,outFile)
	if err != nil{
		d.err = fmt.Errorf("%w", err)
		return
	}
	d.downloadPaths = append(d.downloadPaths, outputFilePath)
}

// combineChunks combines all the downloaded file using goroutine.
func (d *Downloader) combineChunks(fileChunks map[int]*os.File, outFile *os.File) error {
	var w int64
	//maps are not ordered hence using for loop
	for i := 0; i < len(fileChunks); i++ {
		handle := fileChunks[i]
		handle.Seek(0, 0) //We need to seek because read and write cursor are same and the cursor would be at the end.
		written, err := io.Copy(outFile, handle)
		if err != nil {
			return err
		}
		w += written
	}

	log.Printf("Wrote to File : %v, Written bytes : %v", outFile.Name(), w)

	return nil
}

// downloadFileForRange downloads file for the given range.
func (d *Downloader) downloadFileForRange(wg *sync.WaitGroup, url, byteRange string, file io.Writer, waitchan chan struct{}) {

	if wg != nil {
		defer wg.Done()
	}

	request, err := http.NewRequest("GET", url, strings.NewReader(""))
	if err != nil {
		d.err = err
		return
	}

	if byteRange != "" {
		request.Header.Add("Range", "bytes="+byteRange)
	}

	client := http.Client{
		Timeout: 0,
	}

	response, err := client.Do(request)
	if err != nil {
		d.err = err
		return
	}
	defer response.Body.Close()

	defer func() {
		fmt.Println("goroutine is completed")
		<- waitchan
	}()

	if response.StatusCode != 200 && response.StatusCode != 206 {
		d.err = fmt.Errorf("Did not get 20X status code, got : %v", response.StatusCode)
		return
	}

	_, err = io.Copy(file, response.Body)
	if err != nil {
		d.err = fmt.Errorf("error while copying downloded file response to file : %v", err)
		return
	}

}

// checkFileSizeWithHeaderContentLength checks the file length before downloading.
// Based on header content-length.
func checkFileSizeWithHeaderContentLength(fileUrl string) (int64, error) {
	resp, err := http.Head(fileUrl)
	if err != nil {
		return 0, fmt.Errorf("error while using HEAD request for the file: %s and error: %w", fileUrl, err)
	}

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("status is :%d of HEAD request for the file: %s", resp.StatusCode, fileUrl)
	}

	header := resp.Header.Get("Content-Length")
	if header == "" {
		return 0, nil
	}

	size, err := strconv.Atoi(header)
	if err != nil {
		return 0, fmt.Errorf("error while converting string content-length of file to int: %w", err)
	}

	return int64(size), nil
}
