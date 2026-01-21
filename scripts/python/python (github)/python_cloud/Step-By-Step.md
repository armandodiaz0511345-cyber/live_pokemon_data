Phase 0: Download azure fucntion core tools (v4.x Windows 64)
          Download Azure function extension

# Phase 1: The Azure Portal Setup (Infrastructure)

Before writing code, you need the "house" for your code to live in.

### **1. Create the Storage Account**
* Search for **Storage Accounts** > **Create**.
* Name it (e.g., `livepokemondata`).
* Go to the resource > **Access Keys** (on the left menu).
* **Action:** Copy the **Connection String** (starts with `DefaultEndpointsProtocol=https...`). Save this for later.

### **2. Create the SQL Database**
* Ensure your SQL Server is running.
* Go to **Networking** (under Security) in your SQL Server blade.
* **Check the box:** "Allow Azure services and resources to access this server". (Crucial for the Function to talk to SQL).
* **Add your Client IP:** Click "Add your client IPv4 address" so you can connect from home.

### **3. Create the Function App**
* Search for **Function App** > **Create**.
* **Runtime stack:** Python.
* **Version:** 3.10 or 3.11 (Recommended).
* **Hosting:** Consumption (Serverless) is fine for this.
* **Storage:** Link the storage account you created in Step 1.
* 

# Phase 2: Local Project Setup (The "Clean Slate")

This prevents the "Module Not Found" and "Python Not Found" errors.

1. **Create a Folder:** Create a folder named `PokemonDataPipeline` on your desktop.
2. **Open VS Code:** Open this folder in VS Code.
3. **Open Terminal:** Press `Ctrl + ~` or go to `Terminal > New Terminal`.
4. **Create the Virtual Environment:** Run this command to create an isolated "sandbox":
   ```bash
   py -m venv .venv

# Phase 3: The Configuration Files

These files tell Azure "how" to run your code. Create these in your root folder.

### **1. requirements.txt**
Create a file named `requirements.txt` and paste:
```plaintext
azure-functions
azure-storage-blob
pyodbc
requests
```
### **2. host.json**
Create a file named host.json and paste:
```json
{
  "version": "2.0",
  "logging": {
    "applicationInsights": {
      "samplingSettings": {
        "isEnabled": true,
        "excludedTypes": "Request"
      }
    }
  },
  "extensionBundle": {
    "id": "Microsoft.Azure.Functions.ExtensionBundle",
    "version": "[4.*, 5.0.0)"
  }
}
```
### **3. local.settings.json**
Note: Replace PASTE_STORAGE_STRING_HERE (w/ your STORAGE access key connection string) and your SQL details below.
```json

{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "PASTE_STORAGE_STRING_HERE",
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "SqlConnectionString": "Driver={ODBC Driver 18 for SQL Server};Server=tcp:YOUR_SERVER.database.windows.net,1433;Database=YOUR_DB;Uid=YOUR_USER;Pwd=YOUR_PASSWORD;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
  }
}
```

### **Phase 4: Python Code**
```markdown
# Phase 4: The Python Code

1.  **File Creation:** In your main folder, create a file named exactly `function_app.py`.
2.  **Logic:** Paste your Python code for fetching the API data and the SQL connection logic into this file. 
    * *Note: Ensure your code uses the connection strings defined in your `local.settings.json`.*

```
# Phase 5: Run & Troubleshoot

### **How to Run**
1.  **Navigate:** Ensure your terminal is in the main folder (where `host.json` is).
2.  **Execute:** Run the following command:
    ```bash
    func start
    ```
3.  **Monitor:** Watch the logs. You should see "Starting Pokemon Data Pipeline" followed by page requests.

---

### **Troubleshooting Guide**

| Error | Cause | Fix |
| :--- | :--- | :--- |
| **ModuleNotFoundError** | Libraries installed globally, but Azure looks in `.venv`. | Run `.\.venv\Scripts\activate` then `pip install -r requirements.txt`. |
| **Unable to find project root** | Terminal is in the wrong subfolder. | Type `cd ..` until you see `host.json` in your current folder. |
| **Scripts disabled** | Windows blocking `.venv` activation. | Run: `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser`. |
| **Connection Refused** | Using local emulator instead of Azure cloud. | Replace `UseDevelopmentStorage=true` with your real Azure Connection String. |

* as someone that doesnt know much about this yet, Google Gemini was able to get me through all the errors fairly easily.
use your resources!
