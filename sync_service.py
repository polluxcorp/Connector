import win32serviceutil
import win32service
import win32event
import servicemanager
import time
import xmlrpc.client
import mysql.connector
from mysql.connector import Error
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import shutil
import os

class SyncService(win32serviceutil.ServiceFramework):
    _svc_name_ = "OdooMESyncService"
    _svc_display_name_ = "Odoo MES Synchronization Service"
    _svc_description_ = "Service to synchronize data between Odoo and MES system."

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.is_running = True

    def SvcStop(self):
        self.is_running = False
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE, servicemanager.PYS_SERVICE_STARTED, (self._svc_name_, ""))
        self.main()

    def send_failure_email(self, subject, body):
        sender_email = "your-email@example.com"
        receiver_email = "admin@example.com"
        password = "your-email-password"

        message = MIMEMultipart()
        message["From"] = sender_email
        message["To"] = receiver_email
        message["Subject"] = subject

        message.attach(MIMEText(body, "plain"))

        try:
            with smtplib.SMTP("smtp.example.com", 587) as server:
                server.starttls()
                server.login(sender_email, password)
                server.sendmail(sender_email, receiver_email, message.as_string())
                servicemanager.LogInfoMsg("Email sent successfully")
        except Exception as e:
            servicemanager.LogErrorMsg(f"Failed to send email: {e}")

    def backup_database(self):
        backup_dir = "C:\\path\\to\\backup\\dir"  # Use an absolute path
        timestamp = time.strftime("%Y%m%d%H%M%S")
        backup_file = os.path.join(backup_dir, f"backup_{timestamp}.sql")
        compressed_backup_file = backup_file + ".zip"

        try:
            # Create a backup of the database
            os.system(f"mysqldump -u {mysql_user} -p{mysql_password} {mysql_database} > {backup_file}")

            # Compress the backup file
            shutil.make_archive(backup_file, 'zip', backup_dir, backup_file)

            # Remove the uncompressed backup file
            os.remove(backup_file)

            return compressed_backup_file
        except Exception as e:
            self.send_failure_email("Database Backup Failure", f"Failed to create database backup: {e}")
            raise e

    def main(self):
        # Odoo connection details
        odoo_url = 'https://your-odoo-instance.com'
        odoo_db = 'your-database-name'
        odoo_username = 'your-username'
        odoo_password = 'your-password'

        # MySQL connection details
        global mysql_user, mysql_password, mysql_database
        mysql_host = 'localhost'
        mysql_database = 'your-database'
        mysql_user = 'your-username'
        mysql_password = 'your-password'

        def connect_to_odoo():
            odoo_common = xmlrpc.client.ServerProxy(f'{odoo_url}/xmlrpc/2/common')
            odoo_uid = odoo_common.authenticate(odoo_db, odoo_username, odoo_password, {})
            odoo_models = xmlrpc.client.ServerProxy(f'{odoo_url}/xmlrpc/2/object')
            return odoo_models, odoo_uid

        def connect_to_mysql():
            try:
                connection = mysql.connector.connect(
                    host=mysql_host,
                    database=mysql_database,
                    user=mysql_user,
                    password=mysql_password
                )
                if connection.is_connected():
                    return connection
            except Error as e:
                self.send_failure_email("MySQL Connection Failure", f"Failed to connect to MySQL: {e}")
                raise e

        def sync_customer_data(odoo_models, odoo_uid):
            connection = connect_to_mysql()
            if connection:
                cursor = connection.cursor(dictionary=True)
                
                # Sync from MySQL to Odoo
                cursor.execute("SELECT * FROM customers")
                customers = cursor.fetchall()
                for customer in customers:
                    customer_exists = odoo_models.execute_kw(odoo_db, odoo_uid, odoo_password, 'res.partner', 'search_count', [[['name', '=', customer['name']]]])
                    if customer_exists == 0:
                        odoo_models.execute_kw(odoo_db, odoo_uid, odoo_password, 'res.partner', 'create', [{
                            'name': customer['name'],
                            'email': customer['email']
                        }])

                # Sync from Odoo to MySQL
                odoo_customers = odoo_models.execute_kw(odoo_db, odoo_uid, odoo_password, 'res.partner', 'search_read', [[]], {'fields': ['name', 'email']})
                for odoo_customer in odoo_customers:
                    cursor.execute("SELECT COUNT(*) FROM customers WHERE name = %s", (odoo_customer['name'],))
                    if cursor.fetchone()['COUNT(*)'] == 0:
                        cursor.execute("INSERT INTO customers (name, email) VALUES (%s, %s)", (odoo_customer['name'], odoo_customer['email']))
                
                connection.commit()
                connection.close()

        def sync_part_numbers(odoo_models, odoo_uid):
            connection = connect_to_mysql()
            if connection:
                cursor = connection.cursor(dictionary=True)
                
                # Sync from MySQL to Odoo
                cursor.execute("SELECT * FROM part_numbers")
                part_numbers = cursor.fetchall()
                for part in part_numbers:
                    part_exists = odoo_models.execute_kw(odoo_db, odoo_uid, odoo_password, 'product.product', 'search_count', [[['default_code', '=', part['part_number']]]])
                    if part_exists == 0:
                        odoo_models.execute_kw(odoo_db, odoo_uid, odoo_password, 'product.product', 'create', [{
                            'name': part['part_name'],
                            'default_code': part['part_number']
                        }])

                # Sync from Odoo to MySQL
                odoo_parts = odoo_models.execute_kw(odoo_db, odoo_uid, odoo_password, 'product.product', 'search_read', [[]], {'fields': ['name', 'default_code']})
                for odoo_part in odoo_parts:
                    cursor.execute("SELECT COUNT(*) FROM part_numbers WHERE part_number = %s", (odoo_part['default_code'],))
                    if cursor.fetchone()['COUNT(*)'] == 0:
                        cursor.execute("INSERT INTO part_numbers (part_name, part_number) VALUES (%s, %s)", (odoo_part['name'], odoo_part['default_code']))
                
                connection.commit()
                connection.close()

        def sync_data():
            odoo_models, odoo_uid = connect_to_odoo()
            sync_customer_data(odoo_models, odoo_uid)
            sync_part_numbers(odoo_models, odoo_uid)

        while self.is_running:
            try:
                compressed_backup_file = self.backup_database()
                sync_data()
                servicemanager.LogInfoMsg(f"Sync completed successfully. Backup created: {compressed_backup_file}")
            except Exception as e:
                self.send_failure_email("Sync Failure", f"Sync process failed: {e}")
                self.SvcStop()
            time.sleep(3600)  # Sync every hour

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(SyncService)
