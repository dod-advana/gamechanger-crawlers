#!/usr/bin/env python
from cryptography import x509
from cryptography.hazmat.backends import default_backend
import pem
import certifi

# from urllib import request
from subprocess import run, PIPE
import platform
import argparse
from shutil import copyfile
import logging
import os
import networkx as nx

osx_list_cmd = ["security", "find-certificate", "-a", "-p"]
rhel_list_cmd = ["cat", "/usr/share/pki/ca-trust-source/ca-bundle.trust.p11-kit"]
deb_list_cmd = ["cat", "/etc/ssl/certs/ca-certificates.crt"]

if "REQUESTS_CA_BUNDLE" in os.environ:
    list_cmd = ["cat", os.environ["REQUESTS_CA_BUNDLE"]]
elif platform.system() == "Darwin":
    list_cmd = osx_list_cmd
elif "Ubuntu" in platform.dist():
    list_cmd = deb_list_cmd
elif "centos" in platform.dist():
    list_cmd = rhel_list_cmd
else:
    logging.error(
        f"Unsupported OS platform/distribution: \
      {platform.system()}/{platform.dist()}"
    )
    raise SystemExit(
        "Cannot retrieve root certificates for this operating \
      system"
    )


def get_args():
    parser = argparse.ArgumentParser(
        description="Consolidates Python and system root certificate authority \
          certificates for SSL/TLS"
    )
    parser.add_argument(
        "--replace_pem",
        help="!WARNING - can be destructive! Replaces Python's pem file with \
          the updated cert list. Saves a backup in case of errors.",
        action="store_true",
    )
    parser.add_argument(
        "--output_path",
        dest="output_path",
        help="Specify a file path to write certificates to. Defaults to \
          'combined_certs.pem'",
        default="combined_certs.pem",
    )
    parser.add_argument(
        "--backup_path",
        dest="backup_path",
        help="Specify a file path to backup certifi certificates. Defaults to \
          './backup_path.pem'",
        default="backup_path.pem",
    )
    parser.add_argument(
        "--loglevel",
        dest="loglevel",
        help="Set log level. Options: debug, info, warning, error, critical. \
          default: info",
        default="info",
    )
    return parser.parse_args()


class CertInfo(dict):
    def __init__(self, cert, cert_source=None):
        if not cert_source:
            cert_source = ""
        self.source = cert_source
        attributes = x509.load_pem_x509_certificate(cert.as_bytes(), default_backend())
        self.issuer_cn = attributes.issuer.rfc4514_string()
        self.issuer_long_str = ", ".join(
            reversed([f"{s.oid._name}={s.value}" for s in attributes.issuer])
        )
        self.cn = attributes.subject.rfc4514_string()
        self.signature = attributes.signature
        self.serial_number = attributes.serial_number
        self.issue_date = str(attributes.not_valid_before)
        self.expiration_date = str(attributes.not_valid_after)
        self.value_str = cert.as_text()
        self.value_bytes = cert.as_bytes()
        self.cert_attributes = attributes

    def __getattr__(self, name):
        if name in self:
            return self[name]
        else:
            raise AttributeError("Attribute '{}' not defined")

    def __setattr__(self, name, value):
        self[name] = value

    def __delattr__(self, name):
        if name in self:
            del self[name]
        else:
            raise AttributeError("Attribute '{}' not defined")

    def expires_after(self, cert_info):
        return self.cert_attributes.not_valid_after > cert_info.cert_attributes.not_valid_after

    def expires_before(self, cert_info):
        return self.cert_attributes.not_valid_after < cert_info.cert_attributes.not_valid_after

    def to_formatted_str(self):
        return f"""# Subject: {self.cn}
# Issuer: {self.issuer_cn}
# Serial: {self.serial_number}
# Issue date: {self.issue_date}
# Expiration date: {self.expiration_date}
{self.value_str}"""

    def __repr__(self):
        return f"CertInfo(cn='{self.cn}', issuer='{self.issuer_cn}', serial_number='{self.serial_number}', issue_date='{self.issue_date}', expiration_date='{self.expiration_date}', source='{self.source}'"


def deduplicate_certs(list_of_certs, source=None, deduplicated_certs=None):
    """ """
    if not source:
        source = "Unspecified"
    if not deduplicated_certs:
        deduplicated_certs = {}
    for cert in list_of_certs:
        cert_info = CertInfo(cert, source)
        cert_cn = cert_info.cn
        if cert_cn in deduplicated_certs:
            logging.debug(f"found duplicate {cert_cn}")
            if cert_info.expires_after(deduplicated_certs[cert_cn]):
                deduplicated_certs[cert_cn] = cert_info
                logging.debug(
                    f"updated old cert '{cert_cn}' from '{source}' w/ one from \
                      '{cert_info.source}'"
                )
                continue
            else:
                logging.debug(
                    f"Skipping duplicate cert with cn '{cert_cn}' \
                  from '{source}'"
                )
                continue
        else:
            deduplicated_certs[cert_cn] = cert_info
    logging.debug(
        f"Found {len(list_of_certs)-len(deduplicated_certs)} duplicates; \
          keeping {len(deduplicated_certs)}"
    )
    return deduplicated_certs


def sequence_certs(deduplicated_certificates):
    """Certificates must be properly sequenced when writing a CA bundle file.
    For example, if CA2 was signed by CA1, CA1 should preceed CA2 in the
    bundle file. This function establishes a dependency graph between
    certificates and returns a topologically ordered list of certificates
    using the networkx library.
    """
    certificate_graph = nx.DiGraph()
    for cn in deduplicated_certificates:
        cert_info = deduplicated_certificates[cn]
        if cert_info.issuer_cn != cn:
            certificate_graph.add_edge(cert_info.issuer_cn, cn)
        else:
            certificate_graph.add_node(cn)
    sequenced_cns = list(nx.topological_sort(certificate_graph))
    sequenced_certs = []
    for cn in sequenced_cns:
        if cn in deduplicated_certificates:
            sequenced_certs.append(deduplicated_certificates[cn])
        else:
            logging.debug(f"No cert found for {cn} in certificate bundles")
    return sequenced_certs


def write_certs_to_file(certificates, output_path):
    """ """
    logging.debug(
        f"Saving {len(certificates)} certificates to \
          '{output_path}'"
    )
    with open(output_path, "w") as f:
        for cert_info in certificates:
            f.write(cert_info.to_formatted_str())
            f.write("\n")


def setloglevel(loglevel="info"):
    numeric_level = getattr(logging, loglevel.upper())
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {loglevel}")
    logging.basicConfig(level=numeric_level)


def get_python_certs():
    # get python's pem file using certifi
    certifi_cert_path = certifi.where()
    # read file, get list of certs
    certifi_certs = pem.parse_file(certifi_cert_path)
    logging.debug("Number of certificates in certifi cache: {}".format(len(certifi_certs)))
    return certifi_certs, certifi_cert_path


def get_system_certs():
    # get system cert string
    system_cert_str = run(list_cmd, stdout=PIPE).stdout
    # read byte string, get list of certs
    system_certs = pem.parse(system_cert_str)
    logging.debug("Number of certificates in system cache: {}".format(len(system_certs)))
    return system_certs


def main():
    args = get_args()
    replace_pem = args.replace_pem
    output_path = args.output_path
    backup_path = args.backup_path
    setloglevel(args.loglevel)
    certifi_certs, certifi_path = get_python_certs()
    system_certs = get_system_certs()

    # get one of each cert, in order, from both lists
    deduplicated_certs = deduplicate_certs(certifi_certs, f"python (@{certifi_path})")
    deduplicated_certs = deduplicate_certs(system_certs, "system", deduplicated_certs)
    sequenced_certs = sequence_certs(deduplicated_certs)
    if replace_pem:
        logging.debug("Replacing python pem, placing backup in {}".format(backup_path))
        copyfile(certifi.where(), backup_path)
        write_certs_to_file(sequenced_certs, certifi.where())
    else:
        logging.info(
            "Dumping deduplicated and ordered cert info to stdout and not replacing Python's certificate bundle; use `--replace_pem` argument to do so next time\n"
        )
        for cert_info in sequenced_certs:
            logging.info(cert_info)
    if output_path:
        write_certs_to_file(sequenced_certs, output_path)
        logging.info(
            "To use this w/ requests, try `python -c \"import requests;print(requests.get('https://gcr.io', verify='{}'))\"`".format(
                output_path
            )
        )
        logging.info("You should see '<Response [200]>' printed to the console")


if __name__ == "__main__":
    main()
