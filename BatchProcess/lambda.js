let AWS = require('aws-sdk');
const sns = new AWS.SNS();
const s3 = new AWS.S3();
exports.handler = function (event, context, callback) {
	s3.listObjects({
		'Bucket': 'batchprocess.bucket',
		'MaxKeys': 100,
		'Prefix': ''
	}).promise()
		.then(data => {
			console.log(data);           // successful response
			/*
			data = {
			 Contents: [
				{
				   ETag: "\\"70ee1738b6b21e2c8a43f3a5ab0eee71\\"",
				   Key: "example1.jpg",
				   LastModified: <Date Representation>,
				   Owner: {
					  DisplayName: "myname",
					  ID: "12345example25102679df27bb0ae12b3f85be6f290b936c4393484be31bebcc"
				   },
				   Size: 11,
				   StorageClass: "STANDARD"
				},
				{...}
			*/
			let numFiles = data.Contents.length;
			let successCount = 0;
			let failedCount = 0;

			console.log(`${numFiles} files found to process`);

			if (numFiles === 0) {
				// There are no files to process. So notify that.
				exports.sendNotification(
					'Processing Finished',
					'No files found to be processed',
					() => callback(null, "Processing finished without any files & Notification sent"),
					(err) => callback(err, "Processing finished without any files & Notification failed"));
			}

			// For each file, execute the processing
			data.Contents.forEach(file => {
				let fileName = file.Key;

				console.log(`Processing File : ${fileName}`);
				// CUSTOM PROCESSING LOGIC GOES HERE

				// After the processing, delete the file
				s3.deleteObject({
					'Bucket': "batchprocess.bucket",
					'Key': fileName
				}).promise()
					.then(data => {
						console.log(`Successfully deleted file ${fileName}`);
						successCount++;

						if ((successCount + failedCount) === numFiles) {
							// This is the last file. So send the notification.
							let message = `Processing finished. ${successCount} successful and ${failedCount} failed`;

							exports.sendNotification(
								'Processing Finished',
								message,
								() => callback(null, "Processing finished & Notification sent"),
								(err) => callback(err, "Processing finished & Notification failed"));
						}
					})
					.catch(err => {
						console.log(`Failed to delete file : ${fileName}`, err, err.stack);
						failedCount++;

						if ((successCount + failedCount) === numFiles) {
							// This is the last file. So send the notification.
							let message = `Processing finished. ${successCount} successful and ${failedCount} failed`;

							exports.sendNotification(
								'Processing Finished',
								message,
								() => callback(null, "Processing finished & Notification sent"),
								(err) => callback(err, "Processing finished & Notification failed"));
						}
					});

				// TODO: delete the processed file

			});
		})
		.catch(err => {
			// console.log(err, err.stack); // an error occurred
			console.log("Failed to get file list", err, err.stack); // an error occurred
			let message = `Message processing failed due to : ${err}`;

			exports.sendNotification(
				'Processing Failed',
				message,
				() => callback(err, "Failed to get file list"),
				(err1) => callback(err, "Failed to get file list"));


		});


	callback(null, 'Successfully executed');
}

exports.sendNotification = (subject, message, onSuccess, onFailure) => {
	// TODO: send SNS notification

	sns.publish({
		Message: message,
		Subject: subject,
		MessageAttributes: {},
		MessageStructure: 'String',
		TopicArn: 'arn:aws:sns:us-east-1:318300609668:BatchProcess_SNS'
	}).promise()
		.then(data => {
			// your code goes here
			console.log("Successfully published notification");
			onSuccess();
		})
		.catch(err => {
			// error handling goes here
			console.log("Error occurred while publishing notification", err, err.stack);
			onFailure(err);
		});
}