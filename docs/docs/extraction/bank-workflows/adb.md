---
sidebar_position: 2
---

# Asian Development Bank (ADB)

## Insert Projects Workflow

The following highlighted fields are webscraped:

Once the fields are scraped, they are mapped to the **ExtractedProject** data schema, and the object is written to the database:

**id.** Auto-assigned by the database.

**created_at_utc.** Auto-assigned.

**last_updated_at_utc.** Auto-assigned.

**affiliates.** Either the "Implementing Agency" or "Executing Agencies" field; the former is used for older projects, while the latter is used for newer projects and located in the "Contact" table. According to the **[ADB project glossary](https://www.adb.org/who-we-are/access-information/projects-glossary#:~:text=a%20share%20holding.-,Executing%20agencies,-%2D%20The%20government/non)**, the field lists "the government/non-government agencies that are working with ADB to implement the project or program."

**countries.** The table cell corresponding to the "Country/Economy" row header.

**date_actual_close.** The table cell under the "Closing" and "Actual" nested column headers in the "Milestones" table. **[Understood to be](https://www.adb.org/who-we-are/access-information/projects-glossary#:~:text=Closing%20date%20or%20loan%20closure)** the date that ADB actually terminated "the right of the borrower to make withdrawals from the loan acount. Expenditures incurred after the loan closing dates are not financed under the loan."

**date_approved.** The table cell under the "Approval" column header in the "Milestones" table. According to the **[ADB glossary](https://www.adb.org/who-we-are/access-information/projects-glossary#:~:text=Closing%20date%20or%20loan%20closure)**, indicates the date that the project was "approved by ADB's Board of Directors or the relevant ADB authority." The next steps in the project cycle are to sign legal documents and wait until the project becomes effective.

**date_disclosed.** Not provided.

**date_effective.** The table cell under the "Effectivity Date" column header in the "Milestones" table. Represents the date the project legally becomes effective.

**date_original_close.** The table cell under the "Closing" and "Original" nested column headers in the "Milestone" table. Represents the original date that ADB intended to terminate the rights of the borower.

**date_revised_close.** The table cell under the "Closing" and "Revised" nested column headers in the "Milestone" table. Represents a revision of the original finance termination date.

**date_signed.** The table cell under the "Signing Date" column header in the "Milestones" table.

**date_under_appraisal.** The table cell corresponding to the "Concept Clearance" row header. According to the **[ADB project glossary](https://www.adb.org/who-we-are/access-information/projects-glossary#:~:text=assessments%20and%20recommendations.-,Concept%20clearance,-%2D%20A%20preliminary%20fact)**, this field represents "A preliminary fact finding on the project that assesses the likely development impact of the project and what value ADB's participation will have on the project, the sector and the country".

finance_types

name

number

sectors

source

status

task_id

total_amount

total_amount_currency

total_amount_usd

url
