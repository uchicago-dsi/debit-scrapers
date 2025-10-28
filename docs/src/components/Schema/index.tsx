import * as react from "react";

type FieldSchemaProps = {
  name: string;
  description: string;
  type: string;
  allowsBlank: boolean;
  allowsNull: boolean;
};

type TableSchemaProps = {
  fields: FieldSchemaProps[];
};

export function SchemaTable({ fields }: TableSchemaProps): react.JSX.Element {
  return (
    <div className="flex flex-col gap-1 mt-5">
      {fields.map(
        ({ name, description, type, allowsBlank, allowsNull }, idx) => (
          <div key={idx} className="text-sm">
            <div className="flex flex-row gap-2">
              <span className="font-bold">{name}</span>{" "}
              <span className="italic">
                {allowsBlank ? "blank-allowed " : ""}
                {allowsNull ? "nullable " : ""}
                {type}
              </span>
            </div>
            <div>{description}</div>
            <hr style={{ marginBottom: 5, marginTop: 5 }} />
          </div>
        )
      )}
    </div>
  );
}

export const ExtractedJobSchema = [
  {
    name: "id",
    description: "The unique identifier of the job in the database.",
    type: "primary key",
    allowsBlank: false,
    allowsNull: false,
  },
  {
    name: "completed_at_utc",
    description: "The end time of the job in UTC.",
    type: "datetime",
    allowsBlank: false,
    allowsNull: true,
  },
  {
    name: "invocation_id",
    description: "The unique identifier of the external trigger invocation.",
    type: "char",
    allowsBlank: false,
    allowsNull: true,
  },
  {
    name: "results_storage_key",
    description:
      "The result file location in the configured Cloud Storage bucket.",
    type: "text",
    allowsBlank: true,
    allowsNull: false,
  },
  {
    name: "started_at_utc",
    description: "The start time of the job in UTC.",
    type: "datetime",
    allowsBlank: false,
    allowsNull: false,
  },
];

export const ExtractedTaskSchema = [
  {
    name: "id",
    description: "The unique identifier of the task in the database.",
    type: "primary key",
    allowsBlank: false,
    allowsNull: false,
  },
  {
    name: "completed_at_utc",
    description: "The task completion date in UTC.",
    type: "datetime",
    allowsBlank: false,
    allowsNull: true,
  },
  {
    name: "created_at_utc",
    description: "The task creation date in UTC.",
    type: "datetime",
    allowsBlank: false,
    allowsNull: false,
  },
  {
    name: "failed_at_utc",
    description: "The latest task failure date in UTC.",
    type: "datetime",
    allowsBlank: false,
    allowsNull: true,
  },
  {
    name: "job_id",
    description: "The unique identifier of the parent job.",
    type: "foreign key",
    allowsBlank: false,
    allowsNull: false,
  },
  {
    name: "last_error",
    description: "The last error message for the task.",
    type: "char",
    allowsBlank: true,
    allowsNull: false,
  },
  {
    name: "retry_count",
    description: "The number of times the task has been retried.",
    type: "char",
    allowsBlank: false,
    allowsNull: false,
  },
  {
    name: "source",
    description: 'The abbreviation of the parent data source (e.g., "AFDB").',
    type: "char",
    allowsBlank: false,
    allowsNull: false,
  },
  {
    name: "started_at_utc",
    description: "The task start time in UTC.",
    type: "datetime",
    allowsBlank: false,
    allowsNull: true,
  },
  {
    name: "status",
    description:
      'The status of the task. One of "Not Started", "In Progress", "Completed", or "Error".',
    type: "char",
    allowsBlank: false,
    allowsNull: false,
  },
  {
    name: "url",
    description: "The URL of the resource to scrape or process.",
    type: "char",
    allowsBlank: true,
    allowsNull: false,
  },
  {
    name: "workflow",
    description:
      'The task workflow type (e.g., "project-page-scrape", "results-page-scrape").',
    type: "char",
    allowsBlank: false,
    allowsNull: false,
  },
];

export const ExtractedProjectSchema = [
  {
    name: "id",
    description: "The unique identifier of the project in the database.",
    type: "primary key",
    allowsBlank: false,
    allowsNull: false,
  },
  {
    name: "created_at_utc",
    description:
      "The time the project record was created in the database, in UTC.",
    type: "datetime",
    allowsBlank: false,
    allowsNull: false,
  },
  {
    name: "last_updated_at_utc",
    description:
      "The time the project record was last updated in the database, in UTC.",
    type: "datetime",
    allowsBlank: false,
    allowsNull: false,
  },
  {
    name: "affiliates",
    description:
      "The organizations affiliated with the project. Pipe-delimited.",
    type: "char",
    allowsBlank: true,
    allowsNull: false,
  },
  {
    name: "countries",
    description:
      "The countries in which the project is located. Pipe-delimited.",
    type: "char",
    allowsBlank: true,
    allowsNull: false,
  },
  {
    name: "date_actual_close",
    description:
      "The actual end date for project funding. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD.",
    type: "char",
    allowsBlank: true,
    allowsNull: false,
  },
  {
    name: "date_approved",
    description:
      "The date the project funding was approved by the bank. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD.",
    type: "char",
    allowsBlank: true,
    allowsNull: false,
  },
  {
    name: "date_disclosed",
    description:
      "The date the project was disclosed to the public. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD.",
    type: "char",
    allowsBlank: true,
    allowsNull: false,
  },
  {
    name: "date_effective",
    description:
      "The date the project funding became effective. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD.",
    type: "char",
    allowsBlank: true,
    allowsNull: false,
  },
  {
    name: "date_last_updated",
    description:
      "The date the project details were last updated. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD.",
    type: "char",
    allowsBlank: true,
    allowsNull: false,
  },
  {
    name: "date_planned_close",
    description:
      "The original projected end date for project funding. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD.",
    type: "char",
    allowsBlank: true,
    allowsNull: false,
  },
  {
    name: "date_planned_effective",
    description:
      "The estimated start date of the project funding. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD.",
    type: "char",
    allowsBlank: true,
    allowsNull: false,
  },
  {
    name: "date_revised_close",
    description:
      "The revised end date for project funding. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD.",
    type: "char",
    allowsBlank: true,
    allowsNull: false,
  },
  {
    name: "date_signed",
    description:
      "The date the project contract was signed by the bank. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD.",
    type: "char",
    allowsBlank: true,
    allowsNull: false,
  },
  {
    name: "date_under_appraisal",
    description:
      "The date the project came under appraisal by the bank. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD.",
    type: "char",
    allowsBlank: true,
    allowsNull: false,
  },
  {
    name: "finance_types",
    description: "The funding types used for the project. Pipe-delimited.",
    type: "char",
    allowsBlank: true,
    allowsNull: false,
  },
  {
    name: "fiscal_year_effective",
    description:
      "The fiscal year the project became effective. Formatted as YYYY.",
    type: "char",
    allowsBlank: true,
    allowsNull: false,
  },
  {
    name: "name",
    description: "The project name.",
    type: "char",
    allowsBlank: true,
    allowsNull: false,
  },
  {
    name: "number",
    description: "The unique identifier assigned by the parent bank, if any.",
    type: "char",
    allowsBlank: true,
    allowsNull: false,
  },
  {
    name: "sectors",
    description:
      "The economic sectors impacted by the project. Pipe-delimited.",
    type: "char",
    allowsBlank: true,
    allowsNull: false,
  },
  {
    name: "source",
    description: "The abbreviation of the parent data source.",
    type: "char",
    allowsBlank: false,
    allowsNull: false,
  },
  {
    name: "status",
    description: "The current status of the project, as reported by the bank.",
    type: "char",
    allowsBlank: true,
    allowsNull: false,
  },
  {
    name: "task_id",
    description: "The unique identifier of the parent task.",
    type: "foreign key",
    allowsBlank: false,
    allowsNull: false,
  },
  {
    name: "total_amount",
    description: "The total amount of funding awarded to the project.",
    type: "decimal",
    allowsBlank: false,
    allowsNull: true,
  },
  {
    name: "total_amount_currency",
    description: "The currency in which the funds were awarded.",
    type: "char",
    allowsBlank: true,
    allowsNull: false,
  },
  {
    name: "total_amount_usd",
    description: "The total amount of funding awarded to the project in USD.",
    type: "decimal",
    allowsBlank: false,
    allowsNull: true,
  },

  {
    name: "url",
    description: "The URL to the project page on the bank's website.",
    type: "URL",
    allowsBlank: false,
    allowsNull: true,
  },
];
