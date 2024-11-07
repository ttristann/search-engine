TODO:
- keep experimenting with ways to only get the significant text
    - skip tags that have text but does not show in the page
    - find a way to only include text that are from these tags:
        allowed_tags = [
            "h1", "h2", "h3", "h4", "h5", "h6",  # Heading tags
            "p",                                 # Paragraph
            "span",                              # Span
            "a",                                 # Anchor
            "ul", "ol", "li",                    # List tags
            "div",                               # Div
            "header", "footer", "section", "article",  # Semantic structural tags
            "b", "strong", "i", "em",            # Bold and italic emphasis
            "blockquote",                        # Blockquote
            "code",                              # Code snippet
            "table", "tr", "td", "th",           # Table tags
            "figcaption",                        # Figure caption
            "details", "summary"                 # Collapsible content
        ]