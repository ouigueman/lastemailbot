# Hosting TSS Gmail Access on Namecheap

## Prerequisites
1. Namecheap shared hosting account
2. Domain name (already registered)
3. Access to cPanel

## Step 1: Prepare Your Files
1. Download all project files from Replit
2. Create a zip file containing:
   - `app.py` (main application)
   - `main.py` (entry point)
   - `templates/` folder (with login.html and dashboard.html)
   - `static/` folder (with style.css if any)
   - `users.txt` (user authentication file)
   - `gmailaccounts.txt` (Gmail account configuration)
   - `requirements.txt` (create this file - see below)

## Step 2: Create requirements.txt
Create a file named `requirements.txt` with the following content:
```
Flask==2.3.3
Flask-Login==0.6.3
gunicorn==21.2.0
```

## Step 3: Upload to Namecheap
1. Log into your Namecheap cPanel
2. Go to **File Manager**
3. Navigate to `public_html` folder
4. Upload and extract your project files
5. Make sure the files are in the root directory or a subdirectory

## Step 4: Python Setup on Namecheap
### For Shared Hosting:
1. Go to **Python App** in cPanel
2. Click "Create Application"
3. Select Python version (3.8 or higher)
4. Set application URL (your domain or subdomain)
5. Set application root to where you uploaded files
6. Set startup file as `main.py`
7. Click "Create"

### Install Dependencies:
1. In Python App interface, open "Virtual Environment"
2. Run: `pip install -r requirements.txt`

## Step 5: Environment Configuration
1. In Python App settings, add environment variables:
   - `SESSION_SECRET`: Generate a random secret key (use online generator)
   - `FLASK_ENV`: production

## Step 6: File Permissions
Ensure these files have correct permissions:
- `users.txt`: 644 (readable by web server)
- `gmailaccounts.txt`: 644 (readable by web server)
- `app.py`: 644
- `main.py`: 644

## Step 7: Update Gmail App Passwords
Make sure all Gmail accounts in `gmailaccounts.txt` have:
1. 2-factor authentication enabled
2. App passwords generated specifically for this application
3. "Less secure app access" is NOT needed (we use app passwords)

## Step 8: Test the Application
1. Visit your domain
2. Test login with credentials from `users.txt`
3. Test email fetching functionality
4. Verify all filters and auto-refresh work

## Troubleshooting

### Common Issues:
1. **500 Internal Server Error**
   - Check Python app logs in cPanel
   - Verify all dependencies are installed
   - Check file permissions

2. **Gmail Connection Failed**
   - Verify app passwords are correct
   - Check Gmail accounts have 2FA enabled
   - Ensure Gmail accounts are not locked

3. **Login Issues**
   - Verify `users.txt` format: `Entity,Username,Password`
   - Check file permissions
   - Verify SESSION_SECRET is set

### Log Files:
- Check Python app error logs in cPanel
- Monitor access logs for any issues

## Security Notes
1. Keep `users.txt` and `gmailaccounts.txt` secure
2. Use strong passwords
3. Regularly update Gmail app passwords
4. Monitor access logs
5. Consider using HTTPS (SSL certificate)

## Alternative: VPS Hosting
If shared hosting doesn't work well, consider Namecheap VPS:
1. Get a VPS plan
2. Install Python 3.8+
3. Install nginx or Apache
4. Use systemd to run the Flask app
5. Set up reverse proxy

## File Structure Example:
```
public_html/
├── app.py
├── main.py
├── requirements.txt
├── users.txt
├── gmailaccounts.txt
├── templates/
│   ├── login.html
│   └── dashboard.html
└── static/
    └── style.css
```

## Support
- Contact Namecheap support for Python app setup issues
- Check Namecheap knowledge base for Python hosting guides
- Ensure your hosting plan supports Python applications