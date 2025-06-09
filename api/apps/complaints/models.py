"""Data models for complaints and complaint-issue associations.
"""

from django.db import models


class Issue(models.Model):
    """Metadata for an IAM complaint issue type.
    """
    
    class Meta:
        db_table = "issue"
        constraints = [
            models.UniqueConstraint(
                fields=["name",], 
                name="unique_issue_name"
            )
        ]
        
    name = models.TextField(
		db_column="name"
    )
    created_at_utc = models.DateTimeField(
		db_column="created_at_utc",
		auto_now_add=True,
		null=True
    )

class Complaint(models.Model):
    """Metadata for a project complaint.
    """
    
    class Meta:
        db_table = "complaint"
        constraints = [
            models.UniqueConstraint(fields=[
                "project",
                "complaint_name",
                "complaint_status",
                "complaint_url",
                "filing_date"
            ],
                name="unique_complaint"
            )
        ]
    
    # Foreign Keys
    project = models.ForeignKey(
        to="projects.Project",
		db_column="project",
		on_delete=models.CASCADE
    )

    # Relationships
    issues = models.ManyToManyField(
        to="complaints.Issue",
        through="ComplaintIssue"
    )

    # Metadata
    complaint_name = models.CharField(
		db_column="complaint_name", 
		max_length=255, 
		blank=True, 
		null=True
	)
    complaint_status = models.CharField(
		db_column="complaint_status", 
		max_length=50, 
		blank=True, 
		null=True
	)
    complaint_url = models.CharField(
		db_column="complaint_url", 
		max_length=255, 
		blank=True, 
		null=True
	)

    # Filing stage
    filer_name = models.CharField(
		db_column="filer_name", 
		max_length=500, 
		blank=True, 
		null=True
	)
    filing_date = models.DateField(
		db_column="filing_date", 
		max_length=25, 
		null=True
	)

    # Registration stage
    is_registered = models.BooleanField(
		db_column="is_registered", 
		null=True
	)
    registration_start = models.DateField(
		db_column="registration_start", 
		null=True
	)
    registration_end = models.DateField(
		db_column="registration_end", 
		null=True
	)
    registration_status = models.TextField(
		db_column="registration_status", 
		blank=True, 
		null=True
	)
    no_registration_explanation = models.TextField(
		db_column="no_registration_explanation", 
		blank=True, 
		null=True
	)
    issued_registration_report = models.BooleanField(
		db_column="issued_registration_report", 
		null=True
	)

    # Eligibility stage
    is_eligible = models.BooleanField(
		db_column="is_eligible", 
		null=True
	)
    eligibility_start = models.DateField(
		db_column="eligibility_start", 
		null=True
	)
    eligibility_end = models.DateField(
		db_column="eligibility_end", 
		null=True
	)
    eligibility_status = models.CharField(
		db_column="eligibility_status", 
		max_length=50, 
		blank=True, 
		null=True
	)
    no_eligibility_explanation = models.TextField(
		db_column="no_eligibility_explanation", 
		blank=True, 
		null=True
	)
    issued_eligibility_report = models.BooleanField(
		db_column="issued_eligibility_report", 
		null=True
	)

    # Dispute resolution stage
    dispute_resolution_start = models.DateField(
		db_column="dispute_resolution_start", 
		null=True
	)
    dispute_resolution_end = models.DateField(
		db_column="dispute_resolution_end", 
		null=True
	)
    dispute_resolution_status = models.CharField(
		db_column="dispute_resolution_status", 
		max_length=50, 
		blank=True, 
		null=True
	)
    no_dispute_resolution_explanation = models.TextField(
		db_column="no_dispute_resolution_explanation", 
		blank=True, 
		null=True
	)
    issued_dispute_report = models.BooleanField(
		db_column="issued_dispute_report", 
		null=True
	)

    # Monitoring stage
    is_monitored = models.BooleanField(
		db_column="is_monitored", 
		null=True
	)
    monitoring_start = models.DateField(
		db_column="monitoring_start", 
		null=True
	)
    monitoring_end = models.DateField(
		db_column="monitoring_end", 
		null=True
	)
    monitoring_status = models.CharField(
		db_column="monitoring_status", 
		max_length=50, 
		blank=True, 
		null=True
	)
    no_monitoring_explanation = models.TextField(
		db_column="no_monitoring_explanation", 
		blank=True, 
		null=True
	)
    issued_monitoring_report = models.BooleanField(
		db_column="issued_monitoring_report", 
		null=True
	)

    # Compliance resolution stage
    found_non_compliance = models.BooleanField(
		db_column="found_non_compliance", 
		null=True
	)
    issued_compliance_report = models.BooleanField(
		db_column="issued_compliance_report", 
		null=True
	)
    compliance_review_start = models.DateField(
		db_column="compliance_review_start", 
		null=True
	)
    compliance_review_end = models.DateField(
		db_column="compliance_review_end", 
		null=True
	)
    compliance_review_status = models.CharField(
		db_column="compliance_review_status", 
		max_length=50, 
		blank=True, 
		null=True
	)
    no_compliance_review_explanation = models.TextField(
		db_column="no_compliance_review_explanation", 
		blank=True, 
		null=True
	)

    # Final stage
    has_agreement = models.BooleanField(
		db_column="has_agreement", 
		null=True
	)
    date_closed = models.DateField(
		db_column="date_closed", 
		max_length=25, 
		null=True
	)

    # Record metadata
    created_at_utc = models.DateTimeField(
		db_column="created_at_utc", 
		auto_now_add=True, 
		null=True
	)

class ComplaintIssue(models.Model):
    """Links complaints to issues in a many-to-many relationship.
    """

    class Meta:
        db_table = "complaint_issue"
        constraints = [
            models.UniqueConstraint(
                fields=["complaint", "issue"], 
                name="unique_complaint_issue"
            )
        ]

    complaint = models.ForeignKey(
        to="complaints.Complaint", 
		db_column="complaint", 
		on_delete=models.CASCADE
	)
    issue = models.ForeignKey(
        to="complaints.Issue", 
		db_column="issue", 
		on_delete=models.CASCADE
	)
    created_at_utc = models.DateTimeField(
		db_column="created_at_utc", 
		auto_now_add=True, 
		null=True
	)
