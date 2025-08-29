from pathlib import Path
from typing import Generator, IO, Any
import zipfile
from isal import igzip_threaded
from storage.interface import Storage
from storage.lxml.serialization.xmlserializer import MyXmlSerializer


class XmlStorage(Storage):
    def list_netex_files(self) -> list[str]:
        """Return only the list of contained XML(.gz) filenames."""
        if str(self.path).endswith(".xml.gz"):
            return [self.path.name]
        elif str(self.path).endswith(".xml"):
            return [self.path.name]
        elif str(self.path).endswith(".zip"):
            with zipfile.ZipFile(self.path) as zip_file:
                return [
                    zf.filename
                    for zf in zip_file.filelist
                    if zf.filename.lower().endswith((".xml.gz", ".xml"))
                ]
        return []

    def open_netex_file(self) -> Generator[tuple[IO[Any], str], None, None]:
        if str(self.path).endswith(".xml.gz"):
            yield igzip_threaded.open(self.path, "rb", compresslevel=3, threads=3), self.path  # type: ignore
        elif str(self.path).endswith(".xml"):
            yield self.path.open("rb"), self.path.name
        elif str(self.path).endswith(".zip"):
            zip_file = zipfile.ZipFile(self.path)
            for zip_filename in zip_file.filelist:
                l_zip_filename = zip_filename.filename.lower()
                if l_zip_filename.endswith(".xml.gz") or l_zip_filename.endswith(".xml"):
                    yield zip_file.open(zip_filename), str(zip_filename)

    def __init__(self, path: Path, readonly: bool = True):
        if readonly and not path.exists():
            raise

        serializer: MyXmlSerializer = MyXmlSerializer([])

        self.path = path
        self.serializer = serializer

