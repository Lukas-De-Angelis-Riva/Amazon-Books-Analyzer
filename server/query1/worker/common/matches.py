def matching_function(published_date_min, published_date_max, category, title):
    def matches(book):
        return int(book.publishedDate) >= published_date_min and \
            int(book.publishedDate) <= published_date_max and \
            category.lower() in [c.lower() for c in book.categories] and \
            title.lower() in book.title.lower()
    return matches