import requests
import os
import glob
import time
from pathlib import Path
from sqlalchemy import create_engine, text
from urllib.parse import urlparse

def wait_for_superset_api(base_url, max_retries=30, retry_interval=10):
    """Wait for Superset API to be available with retry logic"""
    print("🔍 Checking if Superset API is available...")
    
    for attempt in range(1, max_retries + 1):
        try:
            # Try to reach the login endpoint
            response = requests.get(f"{base_url}/api/v1/security/login", timeout=5)
            
            if response.status_code in [200, 405]:  # 405 is also OK (method not allowed for GET)
                print(f"✅ Superset API is available (attempt {attempt})")
                return True
            else:
                print(f"⚠️  Attempt {attempt}: API returned status {response.status_code}")
                
        except requests.exceptions.RequestException as e:
            print(f"⚠️  Attempt {attempt}: Connection failed - {e}")
        
        if attempt < max_retries:
            print(f"⏳ Waiting {retry_interval} seconds before retry...")
            time.sleep(retry_interval)
        else:
            print(f"❌ Superset API not available after {max_retries} attempts")
            return False
    
    return False

def test_database_connectivity(database_name, sqlalchemy_uri, max_retries=6, retry_interval=10):
    """Test database connectivity before creating connection in production"""
    print(f"🔍 Testing connectivity for database '{database_name}'...")
    
    for attempt in range(1, max_retries + 1):
        try:
            # Determine database type from URI
            uri_lower = sqlalchemy_uri.lower()
            
            # Configure connection arguments based on database type
            connect_args = {}
            engine_kwargs = {
                "pool_timeout": 10,
                "pool_recycle": 3600
            }
            
            if "postgresql" in uri_lower:
                connect_args = {
                    "connect_timeout": 10
                }
            elif "mysql" in uri_lower:
                connect_args = {
                    "connect_timeout": 10
                }
            elif "sqlite" in uri_lower:
                connect_args = {
                    "timeout": 10
                }
            elif "trino" in uri_lower:
                # Trino doesn't use standard timeout parameters
                # Use minimal configuration for Trino
                connect_args = {}
                engine_kwargs = {"pool_recycle": 3600}  # Remove pool_timeout for Trino
            elif "presto" in uri_lower:
                # Presto similar to Trino
                connect_args = {}
                engine_kwargs = {"pool_recycle": 3600}
            elif "oracle" in uri_lower:
                connect_args = {
                    "timeout": 10
                }
            elif "mssql" in uri_lower or "sqlserver" in uri_lower:
                connect_args = {
                    "timeout": 10
                }
            else:
                # For unknown databases, try minimal configuration
                print(f"⚠️  Unknown database type, using minimal connection configuration")
                connect_args = {}
                engine_kwargs = {"pool_recycle": 3600}
            
            # Create engine with appropriate configuration
            if connect_args:
                engine = create_engine(sqlalchemy_uri, connect_args=connect_args, **engine_kwargs)
            else:
                engine = create_engine(sqlalchemy_uri, **engine_kwargs)
            
            # Test the connection with appropriate timeout
            connection_timeout = 30  # Overall timeout for the connection attempt
            start_time = time.time()
            
            with engine.connect() as connection:
                # Use a simple query that works across databases
                if "trino" in uri_lower or "presto" in uri_lower:
                    # Trino/Presto specific query
                    result = connection.execute(text("SELECT 1 as test_column"))
                else:
                    # Standard query for most databases
                    result = connection.execute(text("SELECT 1"))
                
                # Fetch the result to ensure query completed
                result.fetchone()
                
                # Check if we exceeded our timeout
                if time.time() - start_time > connection_timeout:
                    raise Exception(f"Connection test exceeded {connection_timeout}s timeout")
                
            print(f"✅ Database '{database_name}' is accessible (attempt {attempt})")
            engine.dispose()
            return True
            
        except Exception as e:
            error_msg = str(e)
            print(f"⚠️  Attempt {attempt}: Database '{database_name}' connection failed - {error_msg[:150]}...")
            
            # Clean up engine if it was created
            try:
                if 'engine' in locals():
                    engine.dispose()
            except:
                pass
        
        if attempt < max_retries:
            print(f"⏳ Waiting {retry_interval} seconds before retry...")
            time.sleep(retry_interval)
        else:
            print(f"❌ Database '{database_name}' not accessible after {max_retries} attempts")
            return False
    
    return False

def authenticate_superset(base_url, username, password, max_retries=10, retry_interval=10):
    """Authenticate with Superset and return session with tokens"""
    session = requests.Session()
    
    for attempt in range(1, max_retries + 1):
        try:
            print(f"🔐 Authentication attempt {attempt}/{max_retries}...")
            
            # Step 1: Login
            login_response = session.post(
                f"{base_url}/api/v1/security/login",
                json={
                    "username": username,
                    "password": password,
                    "provider": "db",
                    "refresh": True
                },
                timeout=10
            )
            
            if login_response.status_code == 200:
                access_token = login_response.json()['access_token']
                print("✅ Login successful")
                
                # Step 2: Get CSRF token
                print("🛡️  Getting CSRF token...")
                csrf_response = session.get(
                    f"{base_url}/api/v1/security/csrf_token",
                    headers={"Authorization": f"Bearer {access_token}"},
                    timeout=10
                )
                
                if csrf_response.status_code == 200:
                    csrf_token = csrf_response.json()['result']
                    print("✅ CSRF token obtained")
                    return session, access_token, csrf_token
                else:
                    print(f"❌ CSRF token failed: {csrf_response.text}")
            else:
                print(f"❌ Login failed: {login_response.status_code} - {login_response.text}")
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Connection error on attempt {attempt}: {e}")
        
        if attempt < max_retries:
            print(f"⏳ Waiting {retry_interval} seconds before retry...")
            time.sleep(retry_interval)
    
    raise Exception(f"Failed to authenticate after {max_retries} attempts")

def create_database_connection(session, base_url, access_token, csrf_token, database_name, uuid, sqlalchemy_uri):
    """Create a database connection using POST API"""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "X-CSRFToken": csrf_token,
        "Content-Type": "application/json"
    }
    
    print(f"🔗 Creating database connection: {database_name}")
    
    payload = {
        "database_name": database_name,
        "sqlalchemy_uri": sqlalchemy_uri,
        "uuid": uuid,
        "expose_in_sqllab": True,
        "allow_run_async": True,
        "allow_ctas": True,
        "allow_cvas": True,
        "allow_dml": True,
        "allow_multi_schema_metadata_fetch": True,
        "allow_csv_upload": True,
        "allow_file_upload": True
    }
    
    try:
        response = session.post(
            f"{base_url}/api/v1/database/",
            json=payload,
            headers=headers,
            timeout=30
        )
        
        if response.status_code == 201:
            print(f"✅ Database connection '{database_name}' created successfully")
            return True
        elif response.status_code == 422:
            # Check if it's a duplicate database name error
            error_data = response.json()
            if "already exists" in str(error_data).lower() or "duplicate" in str(error_data).lower():
                print(f"⚠️  Database connection '{database_name}' already exists, skipping...")
                return True
            else:
                print(f"❌ Database connection '{database_name}' creation failed: {response.status_code} - {response.text}")
                return False
        else:
            print(f"❌ Database connection '{database_name}' creation failed: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error creating database connection '{database_name}': {e}")
        return False

def create_database_connections_from_env(session, base_url, access_token, csrf_token, is_prod=False):
    """Create database connections from environment variables starting with CONNECTION_"""
    print(f"\n{'='*50}")
    print(f"🔗 Creating Database Connections from Environment")
    print(f"{'='*50}")
    
    connection_vars = {k: v for k, v in os.environ.items() if k.startswith('CONNECTION_')}
    
    if not connection_vars:
        print("⚠️  No CONNECTION_ environment variables found")
        return 0, 0
    
    successful = 0
    failed = 0
    skipped = 0
    
    for env_var, connection_string in connection_vars.items():
        try:
            if ':' not in connection_string:
                print(f"❌ Invalid connection string format for {env_var}: {connection_string}")
                print(f"   Expected format: 'DatabaseName:sqlalchemy_uri'")
                failed += 1
                continue
            
            # Split only on the first colon to handle URIs with colons
            database_name, sqlalchemy_uri = connection_string.split(':', 1)
            uuid, sqlalchemy_uri = sqlalchemy_uri.split(':', 1)
            
            print(f"\n📝 Processing {env_var}: {database_name}")
            print(f"   URI: {sqlalchemy_uri}")
            
            # In production, test database connectivity first
            if is_prod:
                print(f"🔍 Production mode: Testing database connectivity first...")
                if not test_database_connectivity(database_name, sqlalchemy_uri):
                    print(f"❌ Skipping '{database_name}' - database not accessible")
                    skipped += 1
                    continue
                print(f"✅ Database '{database_name}' is accessible, proceeding with connection creation...")
            
            if create_database_connection(session, base_url, access_token, csrf_token, database_name, uuid, sqlalchemy_uri):
                successful += 1
            else:
                failed += 1
                
        except Exception as e:
            print(f"❌ Error processing {env_var}: {e}")
            failed += 1
    
    print(f"\n📊 Database Connections Summary:")
    print(f"✅ Successfully created: {successful}")
    print(f"❌ Failed: {failed}")
    if is_prod:
        print(f"⏭️  Skipped (not accessible): {skipped}")
    
    return successful, failed

def upload_file(session, base_url, access_token, csrf_token, endpoint, file_path):
    """Upload a single file to Superset endpoint"""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "X-CSRFToken": csrf_token
    }
    
    file_name = Path(file_path).name
    print(f"📤 Uploading {file_name} to {endpoint}...")
    
    try:
        with open(file_path, 'rb') as f:
            files = {'formData': (file_name, f, 'application/zip')}
            
            response = session.post(
                f"{base_url}/api/v1/{endpoint}/import",
                files=files,
                headers=headers
            )
        
        if response.status_code == 200:
            print(f"✅ {file_name} uploaded successfully")
            return True
        else:
            print(f"❌ {file_name} upload failed: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error uploading {file_name}: {e}")
        return False

def import_superset_files(
    base_url=None, 
    username=None, 
    password=None
):
    """Import all Superset files in correct dependency order"""
    
    # Use environment variables if not provided
    base_url = base_url or os.getenv('SUPERSET_BASE_URL', 'http://localhost:8089')
    username = username or os.getenv('SUPERSET_USERNAME', 'superset_admin')
    password = password or os.getenv('SUPERSET_PASSWORD', 'superset')
    
    # Check if we're in production environment
    is_prod = os.environ.get('env', '').lower() == 'prod'
    
    print(f"🎯 Target Superset: {base_url}")
    print(f"👤 Username: {username}")
    print(f"🌍 Environment: {'PRODUCTION' if is_prod else 'DEVELOPMENT'}")
    
    if is_prod:
        print("⚠️  Production mode: Database connectivity will be tested before creating connections")
    
    try:
        # Wait for API to be available first
        if not wait_for_superset_api(base_url):
            print("❌ Superset API is not available. Exiting.")
            return
        
        # Authenticate
        session, access_token, csrf_token = authenticate_superset(base_url, username, password)
        
        # Get script directory
        script_dir = Path(__file__).parent
        
        # In production, create database connections from environment variables first
        db_success, db_failed = create_database_connections_from_env(session, base_url, access_token, csrf_token, is_prod=True)
        
        # Define import order and file patterns
        import_steps = [
            ("database", "database_*.zip"),
            ("dataset", "dataset_*.zip"),
            ("chart", "chart_*.zip"),
            ("dashboard", "dashboard_*.zip")
        ]
        
        total_uploaded = 0
        total_failed = 0
        
        # Process each import step
        for endpoint, file_pattern in import_steps:
            print(f"\n{'='*50}")
            print(f"📁 Processing {endpoint.upper()} files...")
            print(f"{'='*50}")
            
            # Find all files matching pattern
            files = glob.glob(str(script_dir / file_pattern))
            files.sort()  # Process in alphabetical order
            
            # In production, exclude the specific database export file
            if is_prod and endpoint == "database":
                files = [f for f in files if not Path(f).name.startswith("database_export_20250831T122711")]
                if len(glob.glob(str(script_dir / file_pattern))) > len(files):
                    print("⚠️  Skipping database_export_20250831T122711.zip in production environment")
            
            if not files:
                print(f"⚠️  No {endpoint} files found matching pattern: {file_pattern}")
                continue
            
            print(f"Found {len(files)} {endpoint} file(s) to process")
            
            # Upload each file
            for file_path in files:
                success = upload_file(session, base_url, access_token, csrf_token, endpoint, file_path)
                if success:
                    total_uploaded += 1
                else:
                    total_failed += 1
        
        # Summary
        print(f"\n{'='*50}")
        print(f"📊 IMPORT SUMMARY")
        print(f"{'='*50}")

        print(f"🔗 Database connections created: {db_success}")
        print(f"❌ Database connection failures: {db_failed}")
        
        print(f"✅ Successfully uploaded: {total_uploaded}")
        print(f"❌ Failed uploads: {total_failed}")
        print(f"🎯 Total processed: {total_uploaded + total_failed}")
        
        if total_failed == 0 and (not is_prod or db_failed == 0):
            print("🎉 All imports completed successfully!")
        else:
            print("⚠️  Some imports failed. Check the logs above for details.")
            
    except Exception as e:
        print(f"❌ Fatal error: {e}")

if __name__ == "__main__":
    print("🚀 Starting Superset Import Process...")
    print(f"⏰ Current time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Run the import process
    import_superset_files()