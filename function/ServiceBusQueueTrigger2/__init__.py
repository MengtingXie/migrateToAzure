import logging
import azure.functions as func
import psycopg2
import os
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def main(msg: func.ServiceBusMessage):

    notification_id = int(msg.get_body().decode('utf-8'))
    logging.info('Python ServiceBus queue trigger processed message: %s',notification_id)

    #Get connection to database
    dbConnection = psycopg2.connect(dbname="technoconfdb", user="udacity@migratepostgres35", password="migrate34!", host="migratepostgres35.postgres.database.azure.com")
    cursor = dbConnection.cursor()
    try:
        #Get notification message and subject from database using the notification_id
        logging.info('Get notification message and subject from database using the notification_id')
        cursor.execute("SELECT message, subject FROM notification WHERE id = {};".format(notification_id))
        notification = cursor.fetchone()
        #Get attendees email and name
        logging.info('Get attendees email and name')
        cursor.execute("SELECT first_name, last_name, email FROM attendee;")
        
        #Loop through each attendee and send an email with a personalized subject
        logging.info('Loop through each attendee and send an email')
        attendees = cursor.fetchall()
        for attendee in attendees:
            personalizedSubject = '{} {}:{}'.format(attendee[0],attendee[1],notification[1])
            message = Mail (
                from_email='admin@example.com',
                to_emails=attendee[1],
                subject=personalizedSubject,
                plain_text_content=notification[0])    
            
            #sg = SendGridAPIClient(app.config.get('SENDGRID_API_KEY'))
            #sg.send(message)
        
        #Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        logging.info('Update the notification table')
        completed_date = datetime.utcnow()
        status = 'Notified {} attendees'.format(len(attendees))
        cursor.execute("UPDATE notification SET status = '{}', completed_date = '{}' WHERE id = {};".format(status, completed_date, notification_id))
        dbConnection.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
        dbConnection.rollback()
    finally:
        #Close connection
        cursor.close()
        dbConnection.close()



#def send_email(email, subject, body):
#    if not app.config.get('SENDGRID_API_KEY')
#        message = Mail(
#            from_email=app.config.get('ADMIN_EMAIL_ADDRESS'),
#            to_emails=email,
#            subject=subject,
#            plain_text_content=body)

#        sg = SendGridAPIClient(app.config.get('SENDGRID_API_KEY'))
 
#       sg.send(message)