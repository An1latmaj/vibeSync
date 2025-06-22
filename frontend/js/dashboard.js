// dashboard.js - Handles dashboard functionality for displaying music stats

document.addEventListener('DOMContentLoaded', () => {
    // DOM Elements
    const userNameElement = document.getElementById('user-name');
    const categorySelect = document.getElementById('category');
    const timeRangeSelect = document.getElementById('time-range');
    const customRangeDiv = document.getElementById('custom-range');
    const startDateInput = document.getElementById('start-date');
    const endDateInput = document.getElementById('end-date');
    const topNInput = document.getElementById('top-n');
    const queryBtn = document.getElementById('query-btn');
    const resultCategorySpan = document.getElementById('result-category');
    const resultsTimeRangeSpan = document.getElementById('results-time-range');
    const resultsListDiv = document.getElementById('results-list');

    // Get username from URL or localStorage
    const urlParams = new URLSearchParams(window.location.search);
    let username = urlParams.get('username');

    if (!username) {
        username = localStorage.getItem('vibesync_username');
    }

    if (!username) {
        // Redirect to upload page if no username
        window.location.href = 'upload.html';
        return;
    }

    // Initialize dashboard
    initializeDashboard();

    function initializeDashboard() {
        // Set username display
        userNameElement.textContent = username;

        // Set default dates
        const today = new Date();
        const lastYear = new Date();
        lastYear.setFullYear(today.getFullYear() - 1);

        startDateInput.valueAsDate = lastYear;
        endDateInput.valueAsDate = today;

        // Initialize event listeners
        timeRangeSelect.addEventListener('change', handleTimeRangeChange);
        queryBtn.addEventListener('click', fetchResults);

        // Load initial results
        fetchResults();
    }

    function handleTimeRangeChange() {
        const timeRange = timeRangeSelect.value;

        if (timeRange === 'custom') {
            customRangeDiv.style.display = 'grid';
        } else {
            customRangeDiv.style.display = 'none';
        }
    }

    async function fetchResults() {
        // Update UI
        resultCategorySpan.textContent = categorySelect.options[categorySelect.selectedIndex].text;
        resultsTimeRangeSpan.textContent = getTimeRangeText();

        // Show loading state
        resultsListDiv.innerHTML = `
            <div class="loading-indicator">
                <div class="loader"></div>
                <p>Loading your stats...</p>
            </div>
        `;

        try {
            const params = {
                username: username,
                top_n: parseInt(topNInput.value),
                category: categorySelect.value,
                ...getDateRange()
            };

            const results = await api.getTopItems(params);
            displayResults(results);
        } catch (error) {
            console.error('Error fetching results:', error);
            resultsListDiv.innerHTML = `
                <div class="error-message">
                    <p>Error loading your stats. Please try again.</p>
                    <p class="error-details">${error.message || ''}</p>
                </div>
            `;
        }
    }

    function getDateRange() {
        const timeRange = timeRangeSelect.value;
        const now = new Date();

        switch (timeRange) {
            case 'all-time':
                return {
                    start_time: new Date(2000, 0, 1).toISOString(),
                    end_time: now.toISOString()
                };

            case 'this-year':
                return {
                    start_time: new Date(now.getFullYear(), 0, 1).toISOString(),
                    end_time: now.toISOString()
                };

            case 'last-year':
                return {
                    start_time: new Date(now.getFullYear() - 1, 0, 1).toISOString(),
                    end_time: new Date(now.getFullYear() - 1, 11, 31).toISOString()
                };

            case 'custom':
                return {
                    start_time: startDateInput.value ? new Date(startDateInput.value).toISOString() : null,
                    end_time: endDateInput.value ? new Date(endDateInput.value + 'T23:59:59').toISOString() : null
                };

            default:
                return {};
        }
    }

    function getTimeRangeText() {
        const timeRange = timeRangeSelect.value;

        switch (timeRange) {
            case 'all-time':
                return 'All Time';
            case 'this-year':
                return `${new Date().getFullYear()}`;
            case 'last-year':
                return `${new Date().getFullYear() - 1}`;
            case 'custom':
                if (startDateInput.value && endDateInput.value) {
                    return `${formatDate(startDateInput.value)} - ${formatDate(endDateInput.value)}`;
                }
                return 'Custom Range';
            default:
                return '';
        }
    }

    function formatDate(dateString) {
        const date = new Date(dateString);
        return date.toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' });
    }

    function displayResults(results) {
        if (!results || results.length === 0) {
            resultsListDiv.innerHTML = `
                <div class="no-results">
                    <p>No data found for this time period.</p>
                </div>
            `;
            return;
        }

        let html = '';

        results.forEach((item, index) => {
            html += `
                <div class="result-item">
                    <div class="result-rank">${index + 1}</div>
                    <div class="result-info">
                        <div class="result-name">${item.name}</div>
                        <div class="result-plays">${item.play_count} plays</div>
                    </div>
                </div>
            `;
        });

        resultsListDiv.innerHTML = html;
    }
});