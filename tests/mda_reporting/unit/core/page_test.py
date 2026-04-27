from mda_query_engine.analyze.metadata.time_series_expression import TimeSeriesSelector
from mda_reporting.aggregations.histogram import HistogramDuration
from mda_reporting.core.page import Page
from mda_reporting.core.page_footer import PageFooter
from mda_reporting.core.page_header import PageHeader


def test_page_init():
    """Test Page initialization"""
    page = Page(page_number=1)

    assert page.page_number == 1
    assert page.aggregations == []
    assert isinstance(page.header, PageHeader)
    assert isinstance(page.footer, PageFooter)


def test_add_aggregation():
    """Test adding aggregations to page"""
    page = Page(page_number=1)
    hist = HistogramDuration(name="test_hist", base_expr=TimeSeriesSelector(None), bins=[0.0, 1.0])

    page.add_aggregation(hist)
    assert len(page.aggregations) == 1
    assert page.aggregations[0] == hist
    assert page.aggregations[0].page_number == 1


def test_add_multiple_aggregations():
    """Test adding multiple aggregations"""
    page = Page(page_number=1)
    hist1 = HistogramDuration(name="hist1", base_expr=TimeSeriesSelector(None), bins=[0.0, 1.0])
    hist2 = HistogramDuration(name="hist2", base_expr=TimeSeriesSelector(None), bins=[0.0, 2.0])

    page.add_aggregation(hist1)
    page.add_aggregation(hist2)

    assert len(page.aggregations) == 2
    assert page.aggregations[0] == hist1
    assert page.aggregations[1] == hist2


def test_set_page_number():
    """Test setting page number"""
    page = Page(page_number=1)
    page.set_page_number(3)
    assert page.page_number == 3


def test_get_page_number():
    """Test getting page number"""
    page = Page(page_number=2)
    assert page.get_page_number() == 2


def test_set_header():
    """Test setting custom header"""
    page = Page(page_number=1)
    custom_header = PageHeader()

    page.set_header(custom_header)
    assert page.header == custom_header


def test_get_header():
    """Test getting header"""
    page = Page(page_number=1)
    header = page.get_header()
    assert isinstance(header, PageHeader)


def test_set_footer():
    """Test setting custom footer"""
    page = Page(page_number=1)
    custom_footer = PageFooter()

    page.set_footer(custom_footer)
    assert page.footer == custom_footer


def test_get_footer():
    """Test getting footer"""
    page = Page(page_number=1)
    footer = page.get_footer()
    assert isinstance(footer, PageFooter)
