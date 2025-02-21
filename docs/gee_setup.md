# Google Earth Engine Setup Guide

## Prerequisites
1. A Google Cloud account
2. Google Cloud CLI (`gcloud`) installed
3. Earth Engine Python API installed (`pip install earthengine-api`)
4. Access to the Earth Engine project (`ee-chrismihiar`)

## Authentication Steps

1. **Authenticate with Google Cloud Platform**
   ```bash
   gcloud auth application-default login --scopes=https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/earthengine,https://www.googleapis.com/auth/devstorage.full_control
   ```
   - This will open your browser for authentication
   - Credentials will be saved to: `~/.config/gcloud/application_default_credentials.json`

2. **Verify Earth Engine Connection**
   Create and run a test script (`test_ee_connection.py`):
   ```python
   import ee
   import logging

   logging.basicConfig(level=logging.INFO)
   logger = logging.getLogger(__name__)

   def main():
       try:
           # Initialize with project ID
           ee.Initialize(project='ee-chrismihiar')
           logger.info("Successfully initialized Earth Engine")
           
           # Try a simple operation
           image = ee.Image('USGS/SRTMGL1_003')
           logger.info("Successfully loaded test image")
           
           # Print some info about the image
           info = image.getInfo()
           logger.info(f"Image bands: {info['bands']}")
           
           logger.info("All tests passed!")
           
       except Exception as e:
           logger.error(f"Error testing Earth Engine: {e}")
           raise

   if __name__ == '__main__':
       main()
   ```

3. **Run the Test**
   ```bash
   python test_ee_connection.py
   ```
   
   Expected output:
   ```
   INFO:__main__:Successfully initialized Earth Engine
   INFO:__main__:Successfully loaded test image
   INFO:__main__:Image bands: [...]
   INFO:__main__:All tests passed!
   ```

## Troubleshooting

1. **If authentication fails:**
   - Ensure you have the correct project permissions
   - Try clearing your application default credentials:
     ```bash
     rm ~/.config/gcloud/application_default_credentials.json
     ```
   - Repeat the authentication steps

2. **If initialization fails:**
   - Verify your project ID is correct
   - Ensure you have Earth Engine API enabled in Google Cloud Console
   - Check that your account has been added to the Earth Engine project

3. **Common Error Messages:**
   - "Not signed up for Earth Engine": Make sure you've registered for Earth Engine access
   - "Invalid value for [--scopes]": Make sure to include all required scopes in authentication
   - "Project not found": Verify project ID and permissions

## Using in Code

When initializing Earth Engine in your code, always specify the project:

```python
import ee

# Initialize with project ID
ee.Initialize(project='ee-chrismihiar')
```

## Best Practices

1. Always use logging to track initialization and operations
2. Include error handling for Earth Engine operations
3. Verify authentication before running long processes
4. Keep credentials secure and never commit them to version control

## Additional Resources

- [Earth Engine Python API Documentation](https://developers.google.com/earth-engine/guides/python_install)
- [Google Cloud Authentication Guide](https://cloud.google.com/docs/authentication/getting-started)
- [Earth Engine Developer Console](https://code.earthengine.google.com/) 