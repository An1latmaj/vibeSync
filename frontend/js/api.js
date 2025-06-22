// API handling module

const API_BASE_URL = 'http://localhost:8000'; // Change this to your API URL

const api = {
    // Upload files
    async uploadFiles(username, files) {
        const formData = new FormData();
        formData.append('username', username);

        for (const file of files) {
            formData.append('files', file);
        }

        try {
            const response = await fetch(`${API_BASE_URL}/import/files?username=${encodeURIComponent(username)}`, {
                method: 'POST',
                body: formData,
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.message || 'Upload failed');
            }

            return await response.json();
        } catch (error) {
            console.error('Upload error:', error);
            throw error;
        }
    },

    // Check upload status
    async checkStatus(taskId) {
        try {
            const response = await fetch(`${API_BASE_URL}/import/status/${taskId}`);

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.message || 'Status check failed');
            }

            return await response.json();
        } catch (error) {
            console.error('Status check error:', error);
            throw error;
        }
    },

    // Get top items
    async getTopItems(params) {
        try {
            const response = await fetch(`${API_BASE_URL}/top`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(params),
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.message || 'Failed to fetch top items');
            }

            return await response.json();
        } catch (error) {
            console.error('Top items error:', error);
            throw error;
        }
    },

    // Check API health
    async checkHealth() {
        try {
            const response = await fetch(`${API_BASE_URL}/health`);
            return await response.json();
        } catch (error) {
            console.error('Health check error:', error);
            return { status: 'unhealthy', error: error.message };
        }
    }
};