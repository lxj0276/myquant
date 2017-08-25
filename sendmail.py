# -*- coding: utf-8 -*-
"""
Created on Thu Jul 28 16:45:47 2016
发送邮件模块

@author: chenghg
"""
import smtplib  
from email.header import Header 
from email.message import Message

import mimetypes
from email import encoders
from email.message import Message
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import datetime
import os

class SendMail():
    
    def __init__(self):
        self.smtpserver = 'smtp.exmail.qq.com'
        self.username = 'dylan.cheng@invesmart.cn'
        self.password = 'Aa7821783'
        self.sender = 'dylan.cheng@invesmart.cn'


    def SendTo(self,receiver=[],subject="",content=""):
        # 三个参数：第一个为文本内容，第二个 plain 设置文本格式，第三个 utf-8 设置编码
        message = MIMEText(content, 'plain')
        message['Subject'] = Header(subject)
        message['From'] = self.sender
        message['To'] = ";".join(receiver)
        
       
        
        try:
            server = smtplib.SMTP_SSL(self.smtpserver, port='465')
            #smtp.connect(self.smtpserver, port='465')
            server.login(self.username, self.password)
            server.sendmail(self.sender, receiver, message.as_string())
            server.close()
            print("邮件发送成功")
        except Exception as e:
            print("Error: 无法发送邮件")
            print(e)
            
    def SendTo_add(self,path,receiver=[],subject="",content=""):
        #subject:邮件标题
        '''
        path = "C:\\Users\\Dylan\\Desktop\\慧网工作\\交易计划.docx"  
        subject = 'text'
        receiver = ['chenghuagan@163.com']       
        smtpserver = 'smtp.exmail.qq.com'
        username = 'dylan.cheng@invesmart.cn'
        password = 'Aa7821783'
        sender = 'dylan.cheng@invesmart.cn'
        '''
        
        # Create the enclosing (outer) message
        outer = MIMEMultipart()
        outer['Subject'] = subject
        outer['To'] = ";".join(receiver)
        outer['From'] = 'dylan.cheng@invesmart.cn'

        if os.path.isfile(path):
            #path = os.path.join(directory, filename)
            # Guess the content type based on the file's extension.  Encoding
            # will be ignored, although we should check for simple things like
            # gzip'd or compressed files.
            ctype, encoding = mimetypes.guess_type(path)
            if ctype is None or encoding is not None:
                # No guess could be made, or the file is encoded (compressed), so
                # use a generic bag-of-bits type.
                ctype = 'application/octet-stream'
            maintype, subtype = ctype.split('/', 1)
            if maintype == 'text':
                fp = open(path)
                # Note: we should handle calculating the charset
                msg = MIMEText(fp.read(), _subtype=subtype)
                fp.close()
            elif maintype == 'image':
                fp = open(path, 'rb')
                msg = MIMEImage(fp.read(), _subtype=subtype)
                fp.close()
            elif maintype == 'audio':
                fp = open(path, 'rb')
                msg = MIMEAudio(fp.read(), _subtype=subtype)
                fp.close()
            else:
                fp = open(path, 'rb')
                msg = MIMEBase(maintype, subtype)
                msg.set_payload(fp.read())
                fp.close()
                # Encode the payload using Base64
                encoders.encode_base64(msg)
            # Set the filename parameter
            #basename = os.path.basename(path) 

            msg.add_header('Content-Disposition', 'attachment', filename=os.path.basename(path))
            outer.attach(msg)
            content_text = MIMEText(content, 'plain') #邮件内容
            outer.attach(content_text)
            composed = outer.as_string()

            try:  
                server = smtplib.SMTP_SSL()
                server.connect(self.smtpserver, port='465')
                server.login(self.username, self.password)
                server.sendmail(self.sender, receiver, composed)               
                print("邮件发送成功")
            except Exception as e:
                print("Error: 无法发送邮件")
                print(e) 
            finally:  
                server.close()

if __name__ == "__main__":
    send = SendMail()
    date = datetime.datetime.today()  
    date = datetime.datetime.strftime(date,"%Y%m%d")
    content = "测试"
    if len(content) > 0:
        #receiver = ['chenghg@harvestwm.cn','chenghg@jsfund.cn']
        receiver = ['chenghuagan@163.com']
        subject = '%s金贝塔调仓计划'%date
        #send.SendTo(receiver,subject,content) 
        file_name = "C:\\Users\\Dylan\\Desktop\\慧网工作\\交易计划.docx"  
        send.SendTo_add(file_name,receiver,subject,content)