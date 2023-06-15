$(document).ready(function () {

    const searchInput = $('#search-input');
    const bgImage = $('.bg-image');
    
    searchInput.on('focus', () => {
        bgImage.css({
            backkdr: 'blur(6px)',
            '-webkit-filter': 'blur(6px)'
        });
    });
    
      // Check if a character is entered
      searchInput.on('input', () => {
        if (searchInput.val().trim() !== '') {
            // Check if class required is present
            if (searchInput.hasClass('required')) {
                searchInput.removeClass('border-2 border-red-500 required');
                searchInput.attr('placeholder', 'Enter your search query...');
            }
        }
    });
        

    searchInput.on('blur', () => {
        bgImage.css({
            filter: '',
            '-webkit-filter': ''
        });
    });
    
    $('#search-input').on('keydown', function (e) {
        if (e.keyCode == 13) {
            e.preventDefault();
            $('#search-button').click();
        }
    });

    var scrolled = false;
    $(window).on('wheel', function () {
        if ($('#results .w-full').children().length > 0) {
            if ($(document).height() <= $(window).height() && !scrolled) {
                scrolled = true;
                scrollAjax();
            }
        }
    });

    var endReached = false;
    var page = 1;
    var pages;
   $('#search-button').click(function () {

        if ($('#search-input').val().trim() === '') {
            // Display an error message and return early
            $('#results .w-full').empty();
            $('#search-input').attr('placeholder', 'Please enter a search query').addClass('border-2  border-red-500 required');
            return;
        }

        //reset page and endReached for a new search
        page = 1;
        endReached = false;

        $('#results .w-full').empty();
        $('#loader').removeClass('hidden');
        setTimeout(function () {
            $('#loader').addClass('hidden');
            $.ajax({
                //TODO: CHANGE URL TO http://syzygy.cis5550.net/search?page= (when deployed)
                //TODO: not sure if we are adding http or https
                url: 'http://localhost:8080/search?page=' + page,
                type: 'POST',
                data: {
                    query: $('#search-input').val()
                },

                success: function (response) {
                    pages = response.pages;

                    if (response.results?.length == 0) {
                        //no results so display message to user
                        const message = response.message || 'Sorry, no results found. Please try different query';
                        const messageElement = document.createElement('p');
                        messageElement.textContent = message;
                        messageElement.style.fontSize = '24px';
                        messageElement.style.color = 'white';
                        $('#results .w-full').append(messageElement);
                    }
                    else{
                        componentList(response);
                    }
                },
                error: function (error) {
                    $('#results .w-full').append('<li class="text-red-500">Error: ' + error.responseText + '</li>');
                }
            });
        }, 2000);
    });
    $(window).scroll(function () {
        if ($(window).scrollTop() + $(window).height() == $(document).height()) {
            page += 1;
            if (page >= pages || endReached) {
                endReached = true;
                return;
            }
            scrollAjax();
        }
    });

function scrollAjax() {
        /* 
        if (page == pages + 1 || endReached) {
            return;
        }
        */
        if (page >= pages || endReached) {
            return;
        }
        $.ajax({
               //TODO: CHANGE URL TO http://syzygy.cis5550.net/search?page= (when deployed)
                //TODO: not sure if we are adding http or https
            url: 'http://localhost:8080/search?page=' + page,
            type: 'POST',
            data: {
                query: $('#search-input').val()
            },
            success: function (response) {
                componentList(response);
            },
            error: function (error) {
                $('#results .w-full').append('<li class="text-red-500">Error: ' + error.responseText + '</li>');
            }
        });
    }
    
    function componentList(response) {
        for (var i = 0; i < response.results?.length; i++) {
            var listItem = '<a href="' + response.results[i] + '" target="_blank" class="m-3 block p-6 bg-white border border-gray-200 rounded-lg shadow hover:bg-gray-100 dark:bg-gray-800 dark:border-gray-700 dark:hover:bg-gray-700">' +
                '<h5 class="text-1xl font-bold tracking-tight text-gray-900 dark:text-white no-underline">' + response.results[i] + '</h5>' +
                '</a>';

            $('#results .w-full').append(listItem);
        }
    }

});