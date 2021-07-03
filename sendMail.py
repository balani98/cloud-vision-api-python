#__author__ = "divya p"
#__created_date__ = "04 Aug 2018"
#__version__ = "1.2"
#__email__ = "divya.p@nabler.com"
#__description__ = "Generic Script to Send Email"
#__modified_date__ = "26 Aug 2018"
#__modifications__ = "Bug fix of supporting sending email to multiple recipients"
#__modified_date__ = "5 Dec 2018"
#__modifications__ = "Renamed the script name to lib_send_email and added support for CC and BCC"


import os
import sys
import smtplib
import traceback
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import base64


print('Launching Send Email')
try:
	
	#method to process email adding attachments
	def sendemail(sender,to,cc,bcc,subject,body,attachment,mailhub,password,port):
		#password decrypt
		#decoded = base64.b64decode(password)
		#password=decoded.decode("utf-8")
		# Create the enclosing (outer) message
		outer = MIMEMultipart()
		outer['Subject'] = subject
		outer['To'] = to
		outer['Cc'] = cc
		outer['Bcc'] = bcc
		#print(outer['To'])
		outer['From'] = sender
		outer.preamble = 'You will not see this in a MIME-aware mail reader.\n'
		# List of attachments
		if body != None:
			outer.attach(MIMEText(body, 'plain')) 
		# Add the attachments to the message

		if attachment!= None and attachment!='':
			attachments = str(attachment).split(',') 
			for file in attachments:
				try:
					with open(file, 'rb') as fp:
						msg = MIMEBase('application', "octet-stream")
						msg.set_payload(fp.read())
					encoders.encode_base64(msg)
					msg.add_header('Content-Disposition', 'attachment', filename=os.path.basename(file))
					outer.attach(msg)
				except:
					print("Unable to open one of the attachments. Error: ", sys.exc_info()[0])
					raise
		
		composed = outer.as_string()

		# Send the email
		try:
			with smtplib.SMTP(mailhub, port) as server:
				server.ehlo()
				server.starttls()
				server.ehlo()
				server.login(sender, password)
				server.sendmail(sender, to.split(',')+cc.split(',')+bcc.split(','), composed)
				server.close()
			print("Email sent!")
		except:
			print("Unable to send the email. Error: ", sys.exc_info()[0])
			raise

	#main()

		
except IOError:
	print('Error: Could not find file or read the data!'+str(traceback.format_exc()))
                
except Exception as error:
	print('Error Occured!'+str(traceback.format_exc()))
	
#lib_send_email.sendemail('From_Email_Id','To_Email_ids','Cc_Email_ids','Bcc_Email_ids','Subject','Body','Attachments','smtp.office365.com','base64encryptedPassword',MailhubPort)
