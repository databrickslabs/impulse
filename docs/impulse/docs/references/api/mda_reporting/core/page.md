---
sidebar_label: page
title: mda_reporting.core.page
---

## Page

```python
class Page()
```

Represents a page in a report, containing aggregations, header, and footer.


#### \_\_init\_\_

```python
def __init__(page_number: int)
```

Initialize a Page object.

**Arguments**:

- `page_number` (`int`): The page number for this page.

#### add\_aggregation

```python
def add_aggregation(aggregation: Aggregation)
```

Add an aggregation to the page and set its page number and report ID.

**Arguments**:

- `aggregation` (`Aggregation`): The aggregation to add to the page.

**Returns**:

`None`: 

#### set\_page\_number

```python
def set_page_number(page_number: int)
```

Set the page number for this page and update all associated aggregations.

**Arguments**:

- `page_number` (`int`): The new page number to set.

**Returns**:

`None`: 

#### set\_report\_id

```python
def set_report_id(report_id: int)
```

Set the report ID for this page and update all associated aggregations.

**Arguments**:

- `report_id` (`int`): The report identifier to set.

**Returns**:

`None`: 

#### get\_page\_number

```python
def get_page_number() -> int
```

Get the page number of this page.

**Returns**:

`int`: The page number.

#### set\_header

```python
def set_header(header: PageHeader)
```

Set the header for this page.

**Arguments**:

- `header` (`PageHeader`): The header to set for the page.

#### get\_header

```python
def get_header() -> PageHeader
```

Get the header of this page.

**Returns**:

`PageHeader`: The header of the page.

#### set\_footer

```python
def set_footer(footer: PageFooter)
```

Set the footer for this page.

**Arguments**:

- `footer` (`PageFooter`): The footer to set for the page.

**Returns**:

`None`: 

#### get\_footer

```python
def get_footer() -> PageFooter
```

Get the footer of this page.

**Returns**:

`PageFooter`: The footer of the page.

