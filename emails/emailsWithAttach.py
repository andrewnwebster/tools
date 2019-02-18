# -*- coding: utf-8 -*-

import smtplib
from importlib.machinery import SourceFileLoader

from email.mime.multipart import MIMEMultipart
from email.mime.text  import MIMEText
from email.mime.base import MIMEBase
from email.mime.application import MIMEApplication
from email import encoders

#returns 0 if failed
def sendEmail(
				fromSender=None
				, toRecipient=None
				, subject='Subject Template'
				, emailtext=None
				, emailhtml=None
				, attachmentfilePath=None
				, attachmentdisplayName='Attachment_Name_Template'
				, serverConnect=None
				, readReceipt=None
				):
	msg=MIMEMultipart('alternative')

	
	msg['Subject']=subject

	if fromSender is None:
		print('Send Unsuccessful -- No sender')
		return 0

	if toRecipient is None:
		print('Send Unsuccessful -- No recipient')
		return 0

	if emailhtml is not None and emailtext is not None:
		body_html=emailhtml
		msg.attach(MIMEText(body_html, 'html'))
		body_text=emailtext
		msg.attach(MIMEText(body_text, 'text'))
	elif emailtext is not None:
		body_text=emailtext
		msg.attach(MIMEText(body_text, 'text'))
	elif emailhtml is not None:
		body_html=emailhtml
		msg.attach(MIMEText(body_html, 'html'))
	else: #both are NULL
		return 0

	if serverConnect is None:
		print('Send Unsuccessful -- No server connection provided')
		return 0

	if readReceipt is not None:
		msg['Disposition-Notification-To'] = fromSender

	server=serverConnect
	msg['From']=fromSender
	msg['To']=toRecipient

	extensionTemp=attachmentfilePath.split('/')[-1]
	extensionTemp=extensionTemp.split('.')[-1]

	if attachmentfilePath is not None and extensionTemp is not None:
		filename=attachmentfilePath
		with open(filename, "rb") as opened:
			openedfile = opened.read()

		attachedfile = MIMEApplication(openedfile, _subtype = extensionTemp)
		attachedfile.add_header('content-disposition'
									, 'attachment'
									, filename = attachmentdisplayName)
		msg.attach(attachedfile)

	text = msg.as_string()
	server.sendmail(fromSender, toRecipient.split(','), text)
	return 1

