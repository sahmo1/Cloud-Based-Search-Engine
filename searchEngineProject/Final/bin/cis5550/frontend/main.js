document.addEventListener("DOMContentLoaded", function() {
    console.log("DOM fully loaded and parsed");



    const button = document.getElementById('search-button')
    const input = document.getElementById('search-input')




    button.onclick = function() 
    {


        console.log('Button clicked!')


        // Update the content of the id results

        const results = document.getElementById('results')
        results.innerHTML = 'You searched for: ' + input.value

        if (input.value) {
            
            // Get the HTML element by ID
            var element = document.getElementById("results");

            var books = [
            { "title": "The Great Gatsby", "author": "F. Scott Fitzgerald", "year": 1925 },
            { "title": "To Kill a Mockingbird", "author": "Harper Lee", "year": 1960 },
            { "title": "1984", "author": "George Orwell", "year": 1949 },
            { "title": "Pride and Prejudice", "author": "Jane Austen", "year": 1813 }
            ];

            for (var i = 0; i < books.length; i++) {
            // Create a new element for each book title
            var newElement = document.createElement("li");
            newElement.innerHTML = books[i].title;
            
            // Append the new element to the existing element
            element.appendChild(newElement);
            }


            /* 
            var xhr = new XMLHttpRequest();
            // xhr.open('GET', 'URL' + input.value);
            xhr.open('GET', '/search?query=' + encodeURIComponent(input.value));

            xhr.onload = function() {
            if (xhr.status === 200) {
                console.log('Search results received:', xhr.responseText); 

                const searchResults = document.getElementById('search-results');
                searchResults.classList.remove('hidden');

                const resultsList = searchResults.querySelector('ul');
                resultsList.innerHTML = '';

                const resultsArray = xhr.responseText.split('\n');
                //const resultsArray = JSON.parse(xhr.responseText).results;

                
                for (const result of resultsArray) {
                    const li = document.createElement('li');
                    li.textContent = result;
                    resultsList.appendChild(li);
                }
            }
            else {
                console.log('Request failed.  Returned status of ' + xhr.status);
            }
            };
            
            xhr.send();

            */
        }

        button.onclick = search;
        input.addEventListener
        ('keydown', function(event) 
        {
            if (event.key === 'Enter') {
                    event.preventDefault();

                    search();
                }
            }
        )
    };
});
