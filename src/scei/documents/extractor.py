from __future__ import annotations

import csv
import re
from pathlib import Path
from uuid import UUID

from scei.documents.models import (
    DocumentKind,
    ExtractedClaim,
    ExtractedDocument,
    ExtractedSegment,
    SourceLocator,
)
from scei.resolution.normalization import normalize_identifier

BATCH_PATTERN = re.compile(r"\b(?:BATCH|LOT|API|RAW)-?[A-Z0-9][A-Z0-9-]{2,}\b", re.IGNORECASE)
QUANTITY_PATTERN = re.compile(r"\b\d+(?:\.\d+)?\s?(?:kg|g|mg|units?|pcs)\b", re.IGNORECASE)
DATE_PATTERN = re.compile(r"\b(?:20\d{2}[-/]\d{1,2}[-/]\d{1,2}|\d{1,2}[-/]\d{1,2}[-/]20\d{2})\b")


class DocumentExtractor:
    """Extract layout-aware segments and deterministic claims from local documents."""

    def extract(
        self, path: str | Path, document_id: UUID, organization_id: UUID
    ) -> ExtractedDocument:
        file_path = Path(path)
        kind = self._kind(file_path)
        if kind == DocumentKind.TEXT:
            segments = self._text_segments(file_path)
            mime = "text/plain"
        elif kind == DocumentKind.CSV:
            segments = self._csv_segments(file_path)
            mime = "text/csv"
        elif kind == DocumentKind.SPREADSHEET:
            segments = self._xlsx_segments(file_path)
            mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        elif kind == DocumentKind.DOCX:
            segments = self._docx_segments(file_path)
            mime = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        elif kind == DocumentKind.PDF:
            segments, requires_ocr, warnings = self._pdf_segments(file_path)
            claims = self._claims(segments)
            return ExtractedDocument(
                document_id=document_id,
                organization_id=organization_id,
                file_name=file_path.name,
                kind=kind,
                mime_type="application/pdf",
                segments=segments,
                claims=claims,
                requires_ocr=requires_ocr,
                warnings=warnings,
            )
        else:
            return ExtractedDocument(
                document_id=document_id,
                organization_id=organization_id,
                file_name=file_path.name,
                kind=kind,
                mime_type="application/octet-stream",
                warnings=["unsupported_file_type"],
            )
        return ExtractedDocument(
            document_id=document_id,
            organization_id=organization_id,
            file_name=file_path.name,
            kind=kind,
            mime_type=mime,
            segments=segments,
            claims=self._claims(segments),
        )

    @staticmethod
    def _kind(path: Path) -> DocumentKind:
        return {
            ".txt": DocumentKind.TEXT,
            ".md": DocumentKind.TEXT,
            ".csv": DocumentKind.CSV,
            ".xlsx": DocumentKind.SPREADSHEET,
            ".xlsm": DocumentKind.SPREADSHEET,
            ".docx": DocumentKind.DOCX,
            ".pdf": DocumentKind.PDF,
        }.get(path.suffix.casefold(), DocumentKind.UNKNOWN)

    @staticmethod
    def _text_segments(path: Path) -> list[ExtractedSegment]:
        return [
            ExtractedSegment(
                text=line,
                locator=SourceLocator(paragraph=index),
                extraction_method="plain_text",
                confidence=1.0,
            )
            for index, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1)
            if line.strip()
        ]

    @staticmethod
    def _csv_segments(path: Path) -> list[ExtractedSegment]:
        segments = []
        with path.open(newline="", encoding="utf-8-sig") as stream:
            for row_number, row in enumerate(csv.reader(stream), start=1):
                for column, value in enumerate(row, start=1):
                    if value.strip():
                        segments.append(
                            ExtractedSegment(
                                text=value.strip(),
                                locator=SourceLocator(row=row_number, column=column),
                                extraction_method="csv_cell",
                                confidence=1.0,
                            )
                        )
        return segments

    @staticmethod
    def _xlsx_segments(path: Path) -> list[ExtractedSegment]:
        try:
            from openpyxl import load_workbook
        except ImportError as error:
            raise RuntimeError(
                "Excel extraction requires the documents extra: pip install -e .[documents]"
            ) from error
        segments = []
        workbook = load_workbook(path, read_only=True, data_only=True)
        for sheet in workbook.worksheets:
            for row in sheet.iter_rows():
                for cell in row:
                    if cell.value is not None and str(cell.value).strip():
                        segments.append(
                            ExtractedSegment(
                                text=str(cell.value).strip(),
                                locator=SourceLocator(
                                    sheet=sheet.title, row=cell.row, column=cell.column
                                ),
                                extraction_method="xlsx_cell",
                                confidence=1.0,
                            )
                        )
        return segments

    @staticmethod
    def _docx_segments(path: Path) -> list[ExtractedSegment]:
        try:
            from docx import Document
        except ImportError as error:
            raise RuntimeError(
                "DOCX extraction requires the documents extra: pip install -e .[documents]"
            ) from error
        document = Document(path)
        segments = [
            ExtractedSegment(
                text=paragraph.text.strip(),
                locator=SourceLocator(paragraph=index),
                extraction_method="docx_paragraph",
                confidence=1.0,
            )
            for index, paragraph in enumerate(document.paragraphs, start=1)
            if paragraph.text.strip()
        ]
        for table_index, table in enumerate(document.tables, start=1):
            for row_index, row in enumerate(table.rows, start=1):
                for column_index, cell in enumerate(row.cells, start=1):
                    if cell.text.strip():
                        segments.append(
                            ExtractedSegment(
                                text=cell.text.strip(),
                                locator=SourceLocator(
                                    sheet=f"table-{table_index}", row=row_index, column=column_index
                                ),
                                extraction_method="docx_table_cell",
                                confidence=1.0,
                            )
                        )
        return segments

    @staticmethod
    def _pdf_segments(path: Path) -> tuple[list[ExtractedSegment], bool, list[str]]:
        try:
            from pypdf import PdfReader
        except ImportError as error:
            raise RuntimeError(
                "PDF extraction requires the documents extra: pip install -e .[documents]"
            ) from error
        segments = []
        reader = PdfReader(path)
        for page_number, page in enumerate(reader.pages, start=1):
            text = (page.extract_text() or "").strip()
            if text:
                segments.append(
                    ExtractedSegment(
                        text=text,
                        locator=SourceLocator(page=page_number),
                        extraction_method="pdf_text",
                        confidence=0.98,
                    )
                )
        requires_ocr = not segments and len(reader.pages) > 0
        warnings = ["no_embedded_text_ocr_required"] if requires_ocr else []
        return segments, requires_ocr, warnings

    @staticmethod
    def _claims(segments: list[ExtractedSegment]) -> list[ExtractedClaim]:
        claims = []
        for segment in segments:
            for match in BATCH_PATTERN.finditer(segment.text):
                value = match.group(0)
                claims.append(
                    ExtractedClaim(
                        field_name="batch_identifier",
                        value=value,
                        normalized_value=normalize_identifier(value),
                        locator=segment.locator,
                        extraction_method=f"{segment.extraction_method}:regex",
                        confidence=0.93,
                    )
                )
            for match in QUANTITY_PATTERN.finditer(segment.text):
                value = match.group(0)
                claims.append(
                    ExtractedClaim(
                        field_name="quantity",
                        value=value,
                        normalized_value=value.casefold().replace(" ", ""),
                        locator=segment.locator,
                        extraction_method=f"{segment.extraction_method}:regex",
                        confidence=0.9,
                    )
                )
            for match in DATE_PATTERN.finditer(segment.text):
                value = match.group(0)
                claims.append(
                    ExtractedClaim(
                        field_name="date",
                        value=value,
                        normalized_value=value.replace("/", "-"),
                        locator=segment.locator,
                        extraction_method=f"{segment.extraction_method}:regex",
                        confidence=0.88,
                    )
                )
        return claims
