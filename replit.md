# Overview

TSS Gmail Access is a Flask-based web application that provides secure, role-based access to multiple Gmail accounts through IMAP connections. The application features a comprehensive authentication system with entity-based access control, allowing different TSS entities (TSS1, TSS2, TSS3, TSSW) to access their designated Gmail accounts. Users can log in with their credentials and view email data through a clean, responsive dashboard interface.

# User Preferences

Preferred communication style: Simple, everyday language.

# System Architecture

## Web Framework Architecture
The application uses Flask as the core web framework with Flask-Login for authentication and a secure MVC pattern:
- **app.py**: Main application logic with authentication, IMAP connection handling, and email processing
- **main.py**: Application entry point with development server configuration
- **templates/login.html**: Secure login interface with modern design
- **templates/dashboard.html**: Entity-specific dashboard using Jinja2 templating
- **users.txt**: User authentication database (entity, username, password)
- **gmailaccounts.txt**: Gmail account configuration (entity, email, app_password)

## Frontend Design
- **Responsive Design**: Built with Tailwind CSS for mobile-first responsive layout
- **Interactive Elements**: Custom CSS animations and hover effects for enhanced user experience
- **Icon Integration**: Font Awesome icons for visual consistency
- **Form Handling**: JavaScript-enhanced form submission for account selection

## Email Processing Architecture
- **Optimized IMAP Integration**: Direct connection to Gmail's IMAP servers using Python's imaplib with UID-based fetching
- **Performance Optimization**: Fetches only email headers (not full body) and limits to last 20 emails per folder maximum
- **Client-side Filtering**: Real-time search filtering performed in browser for instant results without server requests
- **MIME Decoding**: Custom functions for handling various email encodings and character sets
- **Error Handling**: Comprehensive logging and error management for connection failures

## Security Model
- **Flask-Login Authentication**: Secure session management with user login/logout functionality
- **Entity-Based Access Control**: Users can only access Gmail accounts from their assigned entity
- **TSSW Admin Access**: TSSW users have full access to all entity Gmail accounts
- **App Passwords**: Uses Gmail App Passwords instead of regular passwords for enhanced security
- **File-Based User Management**: Secure user authentication from users.txt file
- **Session Persistence**: Remember user login sessions until explicit logout
- **Environment Variables**: Session secrets with development fallback

## Data Flow
1. User logs in with username and password from users.txt
2. System authenticates and determines user's entity access level
3. Dashboard displays only Gmail accounts available to user's entity
4. User selects Gmail account from entity-filtered dropdown
5. Application establishes IMAP connection using stored credentials from gmailaccounts.txt
6. Email data is fetched and processed for display
7. Results are rendered through Flask templates with comprehensive error handling
8. User sessions persist until explicit logout

# External Dependencies

## Core Dependencies
- **Flask**: Web framework for routing and templating
- **Flask-Login**: Authentication and session management
- **imaplib**: Python standard library for IMAP email access
- **email**: Python standard library for email parsing and MIME handling

## Frontend Dependencies
- **Tailwind CSS**: Utility-first CSS framework loaded via CDN
- **Font Awesome**: Icon library for UI elements loaded via CDN

## Email Service Integration
- **Gmail IMAP**: Direct integration with Gmail's IMAP servers (imap.gmail.com:993)
- **App Passwords**: Requires Gmail App Password authentication for each account

## Development Environment
- **Python Logging**: Built-in logging for debugging and error tracking
- **Flask Development Server**: Hot reloading enabled for development

## Hosting Requirements
- **Port Configuration**: Configured to run on port 5000 with host binding to 0.0.0.0
- **Environment Variables**: Supports SESSION_SECRET environment variable for production security
- **File Permissions**: Requires read access to users.txt and gmailaccounts.txt files

# Entity Access Control System

## User Entities
- **TSS1**: Access to TSS1-specific Gmail accounts only
- **TSS2**: Access to TSS2-specific Gmail accounts only  
- **TSS3**: Access to TSS3-specific Gmail accounts only
- **TSSW**: Administrative access to all entity Gmail accounts

## Authentication Files
- **users.txt**: Format: `Entity,Username,Password` (one per line)
- **gmailaccounts.txt**: Format: `Entity,EmailAddress,AppPassword` (one per line)

## Recent Changes (August 2025)
### Migration to Replit Environment
- ✅ Successfully migrated from Replit Agent to standard Replit environment
- ✅ Installed all required dependencies (Flask, Flask-Login, gunicorn, etc.)
- ✅ Configured proper port binding and server settings

### Real-time Email System Implementation
- ✅ Implemented advanced connection pooling system for efficient IMAP connections
- ✅ Added persistent Gmail connections shared among multiple users
- ✅ Created Server-Sent Events (SSE) for real-time email updates in browser
- ✅ Implemented automatic connection management (connects when users join, disconnects when no users)
- ✅ Added reliable polling system (every 10 seconds) replacing problematic IDLE implementation
- ✅ Fixed Gmail folder categorization using cached category searches
- ✅ Enhanced email fetching to get 50 most recent emails from Inbox and Spam folders
- ✅ Improved error handling and automatic reconnection for dropped connections
- ✅ Optimized performance to handle 10+ concurrent users efficiently

### Previous Features (January 2025)
- ✅ Implemented Flask-Login authentication system
- ✅ Added entity-based access control for Gmail accounts
- ✅ Created secure login interface with modern design
- ✅ Developed entity-specific dashboard with user info display
- ✅ Added persistent session management with remember me functionality
- ✅ Implemented file-based user and Gmail account management
- ✅ Added TSSW administrative access to all entities
- ✅ Enhanced security with proper logout functionality
- ✅ Added Gmail folder categorization (Primary, Promotions, Social, Updates, Forums, Spam)
- ✅ Created color-coded folder badges for visual distinction
- ✅ Added folder type filtering with dropdown selection
- ✅ Enhanced client-side filtering for instant search results
- ✅ Created responsive table layout with separate columns for better readability
- ✅ Enhanced mobile responsiveness with adaptive column display