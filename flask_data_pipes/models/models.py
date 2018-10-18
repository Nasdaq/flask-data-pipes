from ..ext.services import get_service
from ..ext.roles import Role
from ..data_pipes.filetypes import FileType
from datetime import datetime

db = get_service('db')


class DataModel(db.Model):

    __tablename__ = '__etl_data_models'

    name = db.Column(db.Unicode, primary_key=True)
    pipeline = db.Column(db.Unicode)
    pipeline_version = db.Column(db.Integer, primary_key=True)
    pipeline_version_mapping = db.Column(db.LargeBinary)

    directory = db.Column(db.Unicode)
    filename = db.Column(db.Unicode)

    has_upload = db.Column(db.Boolean, default=False)
    has_extract = db.Column(db.Boolean, default=False)
    has_transform = db.Column(db.Boolean, default=False)
    has_load = db.Column(db.Boolean, default=False)

    upload_sha256 = db.Column(db.Unicode)
    extract_sha256 = db.Column(db.Unicode)
    transform_sha256 = db.Column(db.Unicode)
    load_sha256 = db.Column(db.Unicode)

    upload_accept = db.Column(db.Enum(FileType), default=FileType.data, nullable=False)
    upload_role = db.Column(db.Enum(Role), default=Role.superuser, nullable=False)
    upload_meta_schema = db.Column(db.JSON(none_as_null=True))
    upload_active = db.Column(db.Boolean, default=True)

    created = db.Column(db.DateTime, default=datetime.utcnow())
    modified = db.Column(db.DateTime, default=datetime.utcnow(), onupdate=datetime.utcnow())

    def __init__(self, name, pipeline, directory, filename,
                 has_upload, has_extract, has_transform, has_load,
                 upload_sha256, extract_sha256, transform_sha256, load_sha256,
                 upload_accept, upload_role, upload_meta_schema):

        self.name = name
        self.pipeline = pipeline
        self.directory = directory
        self.filename = filename

        self.has_upload = has_upload
        self.has_extract = has_extract
        self.has_transform = has_transform
        self.has_load = has_load

        self.upload_sha256 = upload_sha256
        self.extract_sha256 = extract_sha256
        self.transform_sha256 = transform_sha256
        self.load_sha256 = load_sha256

        self.upload_accept = upload_accept or FileType.data
        self.upload_role = upload_role or Role.superuser
        self.upload_meta_schema = upload_meta_schema

        self.pipeline_version, self.pipeline_version_mapping = self.version_setter(name)

    def version_setter(self, name):
        """Determine versioning information. Compares hash
        values to set changes to version mapping; increments
        semantic version only if hash changes (i.e., version
        not incremented if mapping previously 0)."""

        __fingerprint__ = ['upload_sha256', 'extract_sha256', 'transform_sha256', 'load_sha256']

        mapping = [int(bool(getattr(self, attr, 0))) for attr in __fingerprint__]
        version = 1

        entry = DataModel.query.filter_by(name=name).order_by(DataModel.pipeline_version.desc()).first()
        if entry:
            changes = [(getattr(self, attr, 0) != getattr(entry, attr, 0)) for attr in __fingerprint__]
            mapping = [sum(values) for values in zip(list(entry.pipeline_version_mapping), changes)]
            existed = [bool(i) & bool(j) for i, j in zip(list(entry.pipeline_version_mapping), changes)]
            version = entry.pipeline_version + 1 if sum(existed) > 0 else entry.pipeline_version

        return version, bytes(mapping)

    def __repr__(self):
        return f"<DataModel(name='{self.name}', pipeline_version='{self.pipeline_version}')>"
