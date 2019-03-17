# OCR with AWS

===========================================================================================

Created by:
	Dor Shtarker & Vladimir Shargorodsky

===========================================================================================

Instructions:

	1. choose a unique bucket name and queue name and add them to the userinfo.txt file
	 located inside the LocalApplication folder.

	2. in your aws console, create a role with the premissions:
		a. EC2FullAccess
		b. S3FullAccess
		c. SQSFullAccess
		
	(if you run into problems, try to add AministratorAccess as well).
	copy the Instance Profile ARNs of the role, and add it to the userinfo.txt file.

	3. provide a key pair name and security group id to the userinfo.txt file as well.

	4. make sure your images url and userinfo files are located in the directory you will
	   run the jar from

	5. inside the LocalApplication folder, compile the LocalApplication using the command:
	   $ mvn package

	6. run using "java -jar target/LocalApplication-1.0.jar inputFile n" wherest inputFile
	   is your image urls file
	   and n is the number of images each worker should run ocr on.

	7. wait for completion.

===========================================================================================
