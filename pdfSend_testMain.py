# -*- coding: utf-8 -*-

import smtplib
from importlib.machinery import SourceFileLoader

import imapclient
import imaplib
import email

from email.mime.multipart import MIMEMultipart
from email.mime.text  import MIMEText
from email.mime.base import MIMEBase
from email.mime.application import MIMEApplication
from email import encoders

import pdfSend_testClass as pdfClass
from emails import emailsWithAttach as emattch

import os
import time


def sendEmail(emailParams=None, serverConnect=None):
	msg=MIMEMultipart('alternative')

	msg['From']=emailParams['from']
	msg['Disposition-Notification-To'] = emailParams['from']

	msg['To']=emailParams['to']
	msg['Subject']=emailParams['subject']
	server=serverConnect
	body=emailParams['body']
	text=emailParams['body_2']

	filename=emailParams['attachmentFullPath']
	with open(filename, "rb") as opened:
		openedfile = opened.read()

	attachedfile = MIMEApplication(openedfile, _subtype = "pdf")
	attachedfile.add_header('content-disposition', 'attachment', filename = "ExamplePDF.pdf")
	msg.attach(attachedfile)

		
	msg.attach(MIMEText(body, 'html'))
	msg.attach(MIMEText(text, 'text'))
	
	text = msg.as_string()
	server.sendmail(emailParams['from'], emailParams['to'].split(','), text)
	return 1



def main():

	emailClass=pdfClass.ccePdfAutosendClass()
	outlookConnectLib= \
		SourceFileLoader("module.name", emailClass.credentialsLocation).load_module()

	outlookServerDict=outlookConnectLib.outlookConnect()
	outlookServer=outlookServerDict['serverfile']
	outlookEmailSender=outlookServerDict['senderFriendly']

	#input table import goes here
	inputFileClass=pdfClass.cceEmailListInputClass()
	receipientDF=inputFileClass.importCCESendList()
	print(receipientDF.head())


	#traverse folders
	traverseClass=pdfClass.folderTraverse()
	cceDealerReportsDF=traverseClass.rootDirTraverse()
	print(cceDealerReportsDF.head())

	#as400 query goes here, check update timestamp, dont attempt if current
	#compile input tables here
	#not necessary? ^^ more of a sanity check on list -- 2nd priority

	print(type(receipientDF))
	print(type(cceDealerReportsDF))

	#loop input table here, for each row, send an email 
	finalInputClass=pdfClass.iteratedCompleteInputFile()
	finalInputDF=finalInputClass.joinInputs(
										receipientDF
										, cceDealerReportsDF
										, inputFileClass.inputFileKeys
										, traverseClass.folderTraverseKeys
								)
	print(finalInputDF.head())

	if finalInputDF is not None:
		finalInputDF['test']='FAIL'
		for index,row in finalInputDF.iterrows():
			print('Sending '+ row['subject'])
			try:
				finalInputDF.loc[index,'test']='SUCCESS'
				emattch.sendEmail(outlookEmailSender
					, row['Emails']
					, row['subject']
					, finalInputClass.textemail()
					, finalInputClass.htmlemail()
					, row['filepath']+row['filename']
					, row['filename']
					, outlookServer #serverConnect=None
					, 1 #readReceipt
					)
				print('Send Success: '+ row['subject'])
				print('Waiting 10 seconds...')
				time.sleep(10)
			except:
				print('Send Error -- logging error on '+ row['subject'])
				finalInputDF.loc[index,'test']=sys.exc_info()[0]
		print(finalInputDF.head())

	else:
		print('finalInputDF is NULL')
		os.quit()

	outlookConnectLib.outlookDisconnect(outlookServer)

	#check recipients folder here
	#output recipient records here
	#output send results here

def input_test():
	inputFileClass=pdfClass.cceEmailListInputClass()
	receipientDF=inputFileClass.importCCESendList()
	print(receipientDF.head())

def traverse_test():
	traverseClass=pdfClass.folderTraverse()
	cceDealerReportDF=traverseClass.rootDirTraverse()



def main_test():

	html_body="""\
	<html>
	  <head></head>
	  <body>
		<p><b>Hi!</b><br>
		   How are you?<br>
		   Here is the <a href="http://www.python.org">link</a> you wanted.
		</p>
	  </body>
	</html>
	"""

	html_body_2='''
	Hi
	Hi
	Hi
	'''

	emailParams={
		'to':"awebster@hmausa.com",
		'from':'awebster@msxi.com',
		'subject':'python attachment test',
		'body':html_body,
		'body_2':html_body_2,
		'attachmentFullPath':'//hke.local/HMA/Dept/Customer_Satisfaction/'\
							'Service Business Development/Service Business Improvement/'\
							'_Programs and Vendors/MSXi/_CCE/Reports/'\
							'_CCE Dealer Performance/201802/WE/'\
							'CCE_DlrReport_WE_WE2_AZ019_201802.PDF',
		'credsLoc':'H:/pw/OutlookCreds.py',
	}
	emailClass=pdfClass.ccePdfAutosendClass()
	outlookConnectLib= \
		SourceFileLoader("module.name", emailClass.credentialsLocation).load_module()

	outlookServerDict=outlookConnectLib.outlookConnect()
	outlookServer=outlookServerDict['serverfile']
	sendEmail(emailParams, outlookServer)
	outlookConnectLib.outlookDisconnect(outlookServer)

def undeliverableProcessClassTest():

	emailClass=pdfClass.ccePdfAutosendClass()
	outlookConnectLib= \
		SourceFileLoader("module.name", emailClass.credentialsLocation).load_module()

	responseDict=outlookConnectLib.imapOutlookConnect()
	
	typ,cnt = responseDict['serverfile'].select('INBOX/Undeliverable_CCE')
	print(cnt)
	
	typ, [msg_ids] = responseDict['serverfile'].search(None, 'ALL')
	#for each message in the inbox
	for num in msg_ids.split():
		typ, msg_data = responseDict['serverfile'].fetch(num, '(RFC822)')

		#print subject/to/from email header
		for response_part in msg_data:
			time.sleep(1)
			if isinstance(response_part, tuple):
				email_parser = email.parser.BytesFeedParser()
				email_parser.feed(response_part[1])
				msg = email_parser.close()
				for header in ['subject', 'to', 'from']:
					print('{:^8}: {}'.format(
						header.upper(), msg[header]))

		#display email body, decoded to HTML
		email_message=email.message_from_string(msg_data[0][1].decode('utf-8'))
		for part in email_message.walk():
			if part.get_content_type() == "text/html": # ignore attachments/html
				body = part.get_payload(decode=True)
				print(body.decode('utf-8'))

	'''
	stat,cnt = responseDict['serverfile'].
	select('INBOX/Undeliverable_CCE')
	#stat,dta = responseDict['serverfile'].fetch('1','(BODY.PEEK[TEXT])')
	stat,dta = responseDict['serverfile'].fetch(cnt[0],'(RFC822)')
	email_message=email.message_from_string(dta[0][1].decode('utf-8'))
	for part in email_message.walk():
		if part.get_content_type() == "text/html": # ignore attachments/html
			body = part.get_payload(decode=True)
			print(body.decode('utf-8'))
			#time.sleep(1)
	'''

	outlookConnectLib.imapOutlookDisconnect(responseDict['serverfile'])

main()
time.sleep(60)
undeliverableProcessClassTest()

'''
	emailClass=pdfClass.ccePdfAutosendClass()
	outlookConnectLib= \
		SourceFileLoader("module.name", emailClass.credentialsLocation).load_module()

	refName = ""
	wildcardedMailbox = "*"
	responseDict=outlookConnectLib.imapOutlookConnect()
	undeliverableClass=pdfClass.undeliverableProcess()

	select_dict=responseDict['serverfile'].select_folder('Inbox')

	messages = responseDict['serverfile'].search(['NOT DELETED'])
	response = responseDict['serverfile'].fetch(messages, ['RFC822', 'BODY[TEXT]'])

	#for k,v in select_dict.items():
	#	print(k,v)

	for msgid, data in response.iteritems():
		parsedEmail = email.message_from_string(data['RFC822'])
		body = email.message_from_string(data['BODY[TEXT]'])
		parsedBody = parsedEmail.get_payload(0)
		print(parsedBody)

	#for x in mboxes:
	#	print(x)

	outlookConnectLib.imapOutlookDisconnect(responseDict['serverfile'])
'''