Sumo2S3 - Take Data from SumoLogic and put it into S3
-----------------------------------------------------

export data from Sumo to S3

# Build

	go build

# Run

	# Create your cache folder
	mkdir _cache_folder_
	# edit your config with your favorite text editor
	vi config
	# create your initial statefile (we don't run without one)
	echo "{}" > statefile
	# run it
	./sumo2s3

# Configuration

	{
	  "sumo_api_jobs": "https://api.sumologic.com/api/v1/search/jobs",
	  "open_jobs": 16,
	  "apiKeyConcurrency": 4,
	  "apiKeys": [
	    ["123456789ABC", "ABC...123"]
	  ],
	  "cachefolder": "./exports",
	  "start_date": "2021-10-09T00:00:00Z",
	  "end_date": "2021-12-09T00:00:10Z",
	  "increment":"60s",
	  "aws_region": "us-east-1",
	  "s3_bucket": "my-output-bucket",
	  "s3_tmp_prefix":"tmp",
	  "s3_prefix":"data",
	  "filters": [
	    {
	      "start_date": "1970-10-01T00:00:00Z",
	      "query": "*"
	    }
	  ]
	}

# Todo/Missing Features

* No support for continuous mode. Sumo has a native feature for exporting data coming in to S3.
* internal state locking needs to be re-done
