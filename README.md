## Yellow Pages Web Scraping Project

### 📝 Project Overview
In this project, I scraped data from **Yellow Pages** to collect a large number of business listings, including detailed contact information and social media accounts. The script was designed with robust retry mechanisms and proper debugging to ensure accurate and efficient scraping.

### 🛠️ Tools and Libraries Used
- **Requests**: For making HTTP requests to fetch Yellow Pages listing pages.
- **BeautifulSoup**: For parsing HTML and extracting structured data.
- **Pandas**: To organize and structure the scraped data.
- **Retries**: To ensure data is scraped even when facing connection issues or rate limits.

### 📊 Data Extracted
The data collected from Yellow Pages included a wide range of fields, such as:
- Business name
- Address
- Contact numbers
- Email addresses
- Social media accounts (e.g., Facebook, Twitter, LinkedIn)
- Website URLs
- Business categories
- Reviews and ratings

### 🚀 Features
- **Retry Mechanism**: The script was designed to handle retries in case of network errors or rate-limiting, ensuring data extraction continues smoothly.
- **Two-Step Scraping Process**: First, the script scraped the business listing links, and then it visited each link to collect further detailed information.
- **Comprehensive Data**: Extracted a large number of fields for each business listing, including social media accounts and phone numbers.
- **Error Debugging**: Proper debugging messages were displayed in the output console to provide feedback during the scraping process and assist with troubleshooting.

### ✅ Successful Outcome
The project successfully collected and structured business data from **Yellow Pages**, including **contact information, social media accounts**, and **website details**. The client received a comprehensive dataset, and the use of retries and debugging ensured the scraping process was both reliable and efficient.
