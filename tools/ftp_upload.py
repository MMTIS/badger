import logging
from ftplib import FTP_TLS
import os
import paramiko
from configuration import ftpconns
from utils.aux_logging import log_all


def upload_to_ftps(filepath: str, myconfig: dict[str, str]) -> None:
    # Extract configuration details
    config: dict[str, object] = ftpconns["myconfig"]
    server: str = str(config["server"])
    directory: str = str(config["directory"])
    username: str = str(config["username"])
    password: str = str(config["password"])
    protocol: str = str(config.get("protocol"))
    port: int = int(str(config.get("port", 21)))  # Default to port 21 if not specified

    if protocol == "ftps":
        # Connect to the FTPS server
        ftps = FTP_TLS()
        ftps.connect(server, port)
        ftps.login(username, password)
        ftps.prot_p()  # Secure data connection

        # Change to the specified directory
        ftps.cwd(directory)

        # Get the filename from the filepath
        filename = os.path.basename(filepath)

        # Open the file in binary mode and upload it
        with open(filepath, "rb") as file:
            ftps.storbinary(f"STOR {filename}", file)

        # Close the FTPS connection
        ftps.quit()
    elif protocol == "sftp":
        filename = os.path.basename(filepath)
        # Set up the SFTP client
        transport = paramiko.Transport((server, port))
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        try:
            # Change to the specified directory
            if sftp is not None and directory is not None:
                sftp.chdir(directory)

            # Open the file in binary mode and upload it
            if sftp is not None and filepath is not None:
                with open(filepath, "rb") as file:
                    if filename is not None:
                        sftp.putfo(file, filename)
        finally:
            # Close the SFTP connection
            if sftp is not None:
                sftp.close()
            transport.close()
    else:
        log_all(logging.WARN, "Only ftps and sftp supported")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Compresses a NeTEx file")
    parser.add_argument("file", type=str, help="file name to upload")
    parser.add_argument(
        "myconfig",
        type=str,
        help="Config to use from the ftp conns dict in the configurations",
    )
    args = parser.parse_args()
    upload_to_ftps(args.file, args.myconfig)
