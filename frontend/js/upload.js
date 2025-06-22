// upload.js - Handles file upload functionality for Spotify history files

document.addEventListener('DOMContentLoaded', () => {
    // DOM Elements
    const dropArea = document.getElementById('drop-area');
    const fileInput = document.getElementById('file-upload');
    const fileList = document.getElementById('file-list');
    const uploadBtn = document.getElementById('upload-btn');
    const usernameInput = document.getElementById('username');
    const uploadStatus = document.getElementById('upload-status');
    const statusMessage = document.getElementById('status-message');
    const statusDetails = document.getElementById('status-details');
    const goDashboardBtn = document.getElementById('go-to-dashboard');

    // State variables
    let selectedFiles = [];
    let statusCheckInterval = null;

    // Initialize event listeners
    initializeEventListeners();

    function initializeEventListeners() {
        // Drag and drop events
        ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
            dropArea.addEventListener(eventName, preventDefaults, false);
        });

        ['dragenter', 'dragover'].forEach(eventName => {
            dropArea.addEventListener(eventName, highlight, false);
        });

        ['dragleave', 'drop'].forEach(eventName => {
            dropArea.addEventListener(eventName, unhighlight, false);
        });

        // Handle file drop
        dropArea.addEventListener('drop', handleDrop, false);

        // Handle file browse
        dropArea.querySelector('.browse-link').addEventListener('click', () => {
            fileInput.click();
        });

        fileInput.addEventListener('change', handleFileSelect);

        // Handle upload button click
        uploadBtn.addEventListener('click', handleUpload);

        // Dashboard navigation
        goDashboardBtn.addEventListener('click', (e) => {
            e.preventDefault();
            const username = usernameInput.value.trim();
            window.location.href = `dashboard.html?username=${encodeURIComponent(username)}`;
        });
    }

    // Prevent default behaviors for drag and drop
    function preventDefaults(e) {
        e.preventDefault();
        e.stopPropagation();
    }

    // Highlight drop area when file is dragged over
    function highlight() {
        dropArea.classList.add('dragover');
    }

    // Remove highlight when file leaves drag area
    function unhighlight() {
        dropArea.classList.remove('dragover');
    }

    // Handle file drop event
    function handleDrop(e) {
        const dt = e.dataTransfer;
        const files = dt.files;
        handleFiles(files);
    }

    // Handle files from file input
    function handleFileSelect(e) {
        const files = e.target.files;
        handleFiles(files);
    }

    // Process selected files
    function handleFiles(fileList) {
        // Filter for JSON files
        const jsonFiles = Array.from(fileList).filter(file =>
            file.name.toLowerCase().endsWith('.json')
        );

        if (jsonFiles.length === 0) {
            showError("Please select only JSON files.");
            return;
        }

        // Add to selected files
        selectedFiles = [...selectedFiles, ...jsonFiles];

        // Update file list display
        updateFileList();
    }

    // Update the file list display
    function updateFileList() {
        fileList.innerHTML = '';

        selectedFiles.forEach((file, index) => {
            const fileItem = document.createElement('div');
            fileItem.className = 'file-item';
            fileItem.innerHTML = `
                <span class="file-name">${file.name}</span>
                <span class="remove-file" data-index="${index}">×</span>
            `;
            fileList.appendChild(fileItem);
        });

        // Add remove event listeners
        document.querySelectorAll('.remove-file').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const index = parseInt(e.target.getAttribute('data-index'));
                selectedFiles.splice(index, 1);
                updateFileList();
            });
        });
    }

    // Handle file upload
    async function handleUpload() {
        // Validate input
        const username = usernameInput.value.trim();

        if (!username) {
            showError("Please enter a username.");
            return;
        }

        if (username.length > 15) {
            showError("Username must be 15 characters or less.");
            return;
        }

        if (selectedFiles.length === 0) {
            showError("Please select at least one JSON file.");
            return;
        }

        try {
            // Show upload status
            uploadStatus.style.display = 'block';
            statusMessage.textContent = 'Uploading files...';
            statusDetails.textContent = 'This may take a moment depending on file size.';

            // Disable upload button
            uploadBtn.disabled = true;

            // Upload files using the API
            const result = await api.uploadFiles(username, selectedFiles);

            if (result.task_id) {
                statusMessage.textContent = 'Files uploaded. Processing started...';
                statusDetails.textContent = 'We are analyzing your Spotify history.';

                // Start checking status
                checkUploadStatus(result.task_id, username);
            }
        } catch (error) {
            console.error('Upload error:', error);
            statusMessage.textContent = 'Upload failed';
            statusDetails.textContent = error.message || 'An unexpected error occurred.';

            // Re-enable upload button
            uploadBtn.disabled = false;
        }
    }

    // Check upload status periodically
    function checkUploadStatus(taskId, username) {
        // Clear any existing interval
        if (statusCheckInterval) {
            clearInterval(statusCheckInterval);
        }

        // Set local storage to remember username
        localStorage.setItem('vibesync_username', username);

        // Check status every 3 seconds
        statusCheckInterval = setInterval(async () => {
            try {
                const statusData = await api.checkStatus(taskId);

                // Update status message
                statusMessage.textContent = statusData.message;

                if (statusData.status === 'processing') {
                    statusDetails.textContent = 'This may take a few minutes for large files.';
                } else if (statusData.status === 'completed') {
                    statusDetails.textContent = 'Your data has been processed successfully!';
                    showCompletionUI(username);
                    clearInterval(statusCheckInterval);
                } else if (statusData.status === 'failed') {
                    statusDetails.textContent = 'Please try again or contact support.';
                    uploadBtn.disabled = false;
                    clearInterval(statusCheckInterval);
                }
            } catch (error) {
                console.error('Status check error:', error);
                statusMessage.textContent = 'Error checking status';
                statusDetails.textContent = error.message || 'An unexpected error occurred.';
                uploadBtn.disabled = false;
                clearInterval(statusCheckInterval);
            }
        }, 3000);
    }

    // Show completion UI
    function showCompletionUI(username) {
        goDashboardBtn.style.display = 'block';
        goDashboardBtn.href = `dashboard.html?username=${encodeURIComponent(username)}`;
    }

    // Show error message
    function showError(message) {
        // Show in status area if visible, otherwise alert
        if (uploadStatus.style.display !== 'none') {
            statusMessage.textContent = 'Error';
            statusDetails.textContent = message;
        } else {
            alert(message);
        }
    }
});