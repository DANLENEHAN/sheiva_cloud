"""
Lambda function for putting workout links into the workout link queue.
Requires the following environment variables:
    - WORKOUT_SCRAPE_TRIGGER_QUEUE: url of the workout link SQS queue
    - WORKOUT_LINKS_BUCKET: name of the s3 bucket

	
TODO:

- Create SQS queue for workout links
- Parse message from SQS queue getting the number of links to scrape
- Get workout link buckets
- Divide the number of links to scrape by the number of buckets 'n'
- For each bucket, pop 'n' links and put them into the workout link queue
- Re-save the bucket without the popped links
- Send delete message to the workout scrape trigger queue

"""
