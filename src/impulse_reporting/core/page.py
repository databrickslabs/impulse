from impulse_reporting.aggregations.aggregation import Aggregation
from .page_header import PageHeader
from .page_footer import PageFooter


class Page:
    """Represents a page in a report, containing aggregations, header, and footer."""

    def __init__(self, page_number: int):
        """
        Initialize a Page object.

        Parameters
        ----------
        page_number : int
            The page number for this page.
        """
        self.page_number = page_number
        self.aggregations = []
        self.header = PageHeader()
        self.footer = PageFooter()
        self.report_id = -1  # Default value indicating no report assigned

    def add_aggregation(self, aggregation: Aggregation):
        """
        Add an aggregation to the page and set its page number and report ID.

        Parameters
        ----------
        aggregation : Aggregation
            The aggregation to add to the page.

        Returns
        -------
        None
        """
        self.aggregations.append(aggregation)
        aggregation.set_page_number(self.page_number)
        aggregation.set_report_id(self.report_id)

    def set_page_number(self, page_number: int):
        """
        Set the page number for this page and update all associated aggregations.

        Parameters
        ----------
        page_number : int
            The new page number to set.

        Returns
        -------
        None
        """
        self.page_number = page_number
        for aggregation in self.aggregations:
            aggregation.set_page_number(page_number)

    def set_report_id(self, report_id: int):
        """
        Set the report ID for this page and update all associated aggregations.

        Parameters
        ----------
        report_id : int
            The report identifier to set.

        Returns
        -------
        None
        """
        self.report_id = report_id
        for aggregation in self.aggregations:
            aggregation.set_report_id(report_id)

    def get_page_number(self) -> int:
        """
        Get the page number of this page.

        Returns
        -------
        int
            The page number.
        """
        return self.page_number

    def set_header(self, header: PageHeader):
        """
        Set the header for this page.

        Parameters
        ----------
        header : PageHeader
            The header to set for the page.
        """
        self.header = header

    def get_header(self) -> PageHeader:
        """
        Get the header of this page.

        Returns
        -------
        PageHeader
            The header of the page.
        """
        return self.header

    def set_footer(self, footer: PageFooter):
        """
        Set the footer for this page.

        Parameters
        ----------
        footer : PageFooter
            The footer to set for the page.

        Returns
        -------
        None
        """
        self.footer = footer

    def get_footer(self) -> PageFooter:
        """
        Get the footer of this page.

        Returns
        -------
        PageFooter
            The footer of the page.
        """
        return self.footer
