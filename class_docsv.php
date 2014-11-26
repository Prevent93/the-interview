<?php

class doCSV {
    /**
     * This is the job that will be launched by the Laravel queue...
     *
     * @param Illuminate\Queue\Jobs\Job $job class retrieved from Laravel's queue
     * @param array $data the data that is going to be used for this queue call.
     *
     */
    public function runJob($job, $data)
    {
        $downloadUrl = $data['url'];
        $clientID = (int) $data['client'];

        $file = $this->downloadCSV($downloadUrl);

        $success = $this->insertCSVIntoDatabase($clientID, $file);
    }

    /**
     * Here we have our CSV file that has been downloaded. We need to do the following:
     *
     *      * uncompress the gzip file
     *      * store that uncompressed file somewhere
     *      * go row by row and insert the data into a database.
     *
     *  Important notes:
     *
     *      * the gzip files could be 250MB big
     *      * uncompressed files more then 2GB big
     *      * we want to use as little RAM as possible ;)
     *      * we deploy code with a memory limit of 512MB ... not enough ram to
     *        store all of the CSV file or rows to insert in the database in one go.
     *
     *  Bonus notes:
     *
     *      * chunking inserts into the database would be nice
     *
     *      * str_getcsv has some bugs. as a hint, check out the PHP documentation,
     *        and read the comment from Ryan Rubley. These bugs will be hit with well-formed
     *        CSV files. The file provided will not expose the bugs.
     *
     *      * fopen, fseek and gzopen will be your friends.
     *
     * @param int $clientID the ID of the client that the CSV is for
     * @param string $file  the file that we are going to be importing.
     *
     */
    private function insertCSVIntoDatabase($clientID, $file)
    {
        throw new Exception("There was an error importing the CSV file into the database.");
    }

    /**
     * This is a function that will download a file using curl over HTTP.
     *
     * For the moment, we will just return the path of the compressed file.
     * There is no need to download the CSV file at all \o/
     *
     * @param string $downloadUrl the URL to download the file from.
     *
     */
    private function downloadCSV($downloadUrl)
    {
        $test = round(rand(1, 6));
        echo $test;

        // We are going to throw a random exception sometimes...
        if ($test == 3)
        {
            throw new Exception("Could not pretend to download file");
        }

        $_file = __DIR__ . '/batch_of_urls.csv.gz';
        if (!file_exists($_file)) {
            throw new Exception("Oh noes, it looks like your CSV file is missing");
        }
    }

}
