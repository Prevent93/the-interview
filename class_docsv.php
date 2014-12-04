<?php
ini_set('memory_limit','512M');
/**
 * Commit timestamp - Just cloned repo and set up a clean laravel install - Charlie Sheather - Thu, 04 Dec 2014 11:31:58
 *
 */
class doCSV {
	/**
	 * This is the job that will be launched by the Laravel queue...
	 *
	 * @param Illuminate\Queue\Jobs\Job $job class retrieved from Laravel's queue
	 * @param array $data the data that is going to be used for this queue call.
	 *
	 */

	private $sql = array();
	private $sql_len = 0;
	private $total_rows = 0;
	public function runJob($job, $data)
	{
		$downloadUrl = $data['url'];
		$clientID = (int) $data['client'];

		try
		{
			$time_start = microtime(true);
			$file = $this->downloadCSV($downloadUrl);
				//http://stackoverflow.com/questions/3908477/need-time-efficient-method-of-importing-large-csv-file-via-php-into-multiple-mys?rq=1
			// https://laracasts.com/forum/?p=649-bulk-insert-update/0
			$success = $this->insertCSVIntoDatabase($clientID, $file);
			$time_end = microtime(true);
			echo 'Total Execution Time: ' . ($time_end - $time_start) . ' seconds';
		}
		catch (Exception $e)
		{
			echo 'Some error occured: [' . $e->getMessage() . ' at ' . $e->getLine() . ']';
		}
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
		// no need to keep decompressing during testing
		// $file = $this->decompressGz($file);
		$fp = fopen('batch_of_urls.csv', 'r');
		if ($fp !== false)
		{
			DB::table('csv_data')->truncate();
			//  as a start lets just get this thing going
			$time_start = microtime(true);

			// much faster juut to use php native pdo, which for this type of task is mroe than adequate.
			$pdo = DB::getPdo();
			$pdo->beginTransaction();
			$sql_base = 'INSERT INTO csv_data (client_id, target_url, source_url, anchor_text, source_crawl_date, source_first_found_date, flag_no_follow, flag_image_link, flag_redirect, flag_frame, flag_old_crawl, flag_alt_text, flag_mention, source_citation_flow, source_trust_flow, target_citation_flow, target_trust_flow, source_topical_trust_flow_topic_0, source_topical_trust_flow_value_0, ref_domain_topical_trust_flow_topic_0, ref_domain_topical_trust_flow_value_0) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)';
			$query = $pdo->prepare($sql_base);
			$sql = $sql_base;
			$chunk_size = 5000;

			while (($data = fgetcsv($fp, 1000, ",")) !== FALSE)
			{
				if ($this->sql_len++ == $chunk_size)
				{
					$pdo->commit();
					$this->sql_len = 0;
					$this->total_rows += $chunk_size;

					echo "inserted $chunk_size more\n";
					$time_end = microtime(true);
					echo "Current insert time: " . ($time_end - $time_start) . " seconds\n";
					$pdo->beginTransaction();
				}

				if ($this->total_rows > 50000) break;

				array_unshift($data, $clientID);
				$query->execute($data);

			}

			$time_end = microtime(true);
			echo "Total insert time: " . ($time_end - $time_start) . " seconds\n";
		}
		fclose($fp);
// die;
// 		var_dump($this->parse_csv(
// '123,"4321","21,4234",234,532
// asd,ewf,wef,fww,wef
// wf,wdf,3rf2
// '));
		// throw new Exception("There was an error importing the CSV file into the database.");
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
		return $_file;
	}

	// http://stackoverflow.com/a/3293251
	private function decompressGz($file_name, $return_new_fp = false)
	{
		// Raising this value may increase performance
		$buffer_size = 4096; // read 4kb at a time
		$out_file_name = str_replace('.gz', '', $file_name);

		// Open our files (in binary mode)
		$file = gzopen($file_name, 'rb');
		$out_file = fopen($out_file_name, 'w');

		if ($file === false || $out_file === false )
		{
			throw new Exception('Couldn\'t open input file during uncompression. OR. Couldn\'t open output file during uncompression.', 1);
		}

		// Keep repeating until the end of the input file
		while(!gzeof($file)) {
			// Read buffer-size bytes
			// Both fwrite and gzread and binary-safe
			fwrite($out_file, gzread($file, $buffer_size));
		}

		// Files are done, close input file
		gzclose($file);
		if (!$return_new_fp)
		{
			fclose($out_file);
			return $out_file_name;
		}
		else
		{
			fseek($out_file, 0);
			return $out_file;
		}
	}

// http://php.net/str_getcsv#113220
	//parse a CSV file into a two-dimensional array
	//this seems as simple as splitting a string by lines and commas, but this only works if tricks are performed
	//to ensure that you do NOT split on lines and commas that are inside of double quotes.
	private function parse_csv($str)
	{
		//match all the non-quoted text and one series of quoted text (or the end of the string)
		//each group of matches will be parsed with the callback, with $matches[1] containing all the non-quoted text,
		//and $matches[3] containing everything inside the quotes
		$str = preg_replace_callback('/([^"]*)("((""|[^"])*)"|$)/s', array($this, 'parse_csv_quotes'), $str);

		//remove the very last newline to prevent a 0-field array for the last line
		$str = preg_replace('/\n$/', '', $str);

		//split on LF and parse each line with a callback
		return array_map(array($this, 'parse_csv_line'), explode("\n", $str));
	}

	//replace all the csv-special characters inside double quotes with markers using an escape sequence
	private function parse_csv_quotes($matches)
	{
		if (isset($matches[3]))
		{

			//anything inside the quotes that might be used to split the string into lines and fields later,
			//needs to be quoted. The only character we can guarantee as safe to use, because it will never appear in the unquoted text, is a CR
			//So we're going to use CR as a marker to make escape sequences for CR, LF, Quotes, and Commas.
			$str = str_replace("\r", "\rR", $matches[3]);
			$str = str_replace("\n", "\rN", $str);
			$str = str_replace('""', "\rQ", $str);
			$str = str_replace(',', "\rC", $str);

			//The unquoted text is where commas and newlines are allowed, and where the splits will happen
			//We're going to remove all CRs from the unquoted text, by normalizing all line endings to just LF
			//This ensures us that the only place CR is used, is as the escape sequences for quoted text
			return preg_replace('/\r\n?/', "\n", $matches[1]) . $str;
		}
		
		return $matches[1];
	}

	//split on comma and parse each field with a callback
	private function parse_csv_line($line)
	{
		return array_map(array($this, 'parse_csv_field'), explode(',', $line));
	}

	//restore any csv-special characters that are part of the data
	private function parse_csv_field($field) {
		$field = str_replace("\rC", ',', $field);
		$field = str_replace("\rQ", '"', $field);
		$field = str_replace("\rN", "\n", $field);
		$field = str_replace("\rR", "\r", $field);
		return $field;
	}

}