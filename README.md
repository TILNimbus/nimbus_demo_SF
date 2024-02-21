# Overview

The Citibike demo was created as a standardised demo for the Snowflake SE team to use with prospects and customers. It is designed to be:
- modular - it is a framework made up of vignettes that can be run together for a story-based demo, or separately with minimal coupling
- consistent – all the vignettes use a common data model which makes it easier for the audience to understand;
- simple - easy to set up and run with no VPN required;
- relevant - incorporates up-to-date messaging and features of Snowflake and aligns to our selling motion
- isolated - multiple SEs can be running the demo at the same time; it is isolated from other demos that might be used in the same account;
- repeatable - easily restored to the starting state;
- portable - runs on any Snowflake deployment, across any cloud;
- extensible - easy to add new features and vignettes as they become available; and
- sharable - can be packaged and shared with partners.

# What's new in Citibike V4
While overall the story and flow of the Citibike core demo remains the same, there are several major changes in this release:
- The demo no longer uses the real data from Citibike. Instead, it generates synthetic data for the TRIPS table that is shaped to look like the Citibike data. It retains a yearly seasonality (linked to weather); it has the same shape across the hour of day and day of week; it has the same routes with the same popularity as in the real data.
  - Having the data synthesised now allows us to control the volume of data we want to work with (by default, the demo creates ~35-40M usable TRIP records) and will also allow us to create a data generator function for optional vignettes around continuous data flow (e.g. Snowpipe -> Streams+Tasks).
- The TRIPS data is now loaded from JSON data instead of CSV. This allows us to still show ingestion and easy use of semi-structured data.
- The TRIPS data has additional data fields including rider information (name, DOB, gender) and payment information (pay by phone app vs. card, payment number). This gives more meaningful data for us to obfuscate with data masking.
- The WEATHER data is now taken from the Data Marketplace instead of being loaded. This gives a much better story around consuming data from the marketplace.
- The demo no longer requires access to the `Snowflake Demo Resources` data exchange so you don't need to request membership. You might still want to join the `SE Sandbox` data exchange to demonstrate publishing/sharing data but it's no longer required for the core Citibike demo.
- The demo is now done entirely in Snowsight. You could do it in the old console if you prefer and just run the dashboards in Snowsight (or Tableau, PowerBI, or whatever is relevant for your customer) but we can now showcase the future of Snowflake's UX. A Tableau workbook is provided if you still want to use that.


# Citibike SQL scripts
The scripts for the Citibike demo are in this distribution, under the core folder.


# Setting up your account to run the Citibike demo
Instructions for setting up your account to run the Citibike demo can be found here:
- [Setting up your demo account](./V4_2-Setting-Up-Your-Account)
- [Preparing to deliver the demo](./V4_3-Preparing-for-the-Demo)

# Delivering the demo
A step-by-step walkthrough script of the demo is here:
- [Delivering the demo](./V4_4-Delivering-the-Demo)

# Recording
A recording of the demo delivery can be viewed [here](https://event.on24.com/eventRegistration/console/EventConsoleApollo.jsp?uimode=nextgeneration&eventid=2744328&sessionid=1&key=27ED6B893778D488A9CC81012AFE53EC&contenttype=A&eventuserid=305999&playerwidth=1000&playerheight=650&caller=previewLobby&text_language_id=en&format=fhvideo1&newConsole=true&newTabCon=true).

# Slides
If you are to deliver a presentation in conjunction with your demo, you should build the Snowflake content in your supporting slide deck from the [Snowflake Overview](https://spn.snowflake.com/s/contentdocument/0693r000007D2bpAAC) and [Snowflake Technical Deep Dive](https://spn.snowflake.com/s/contentdocument/0693r00000858ouAAA) decks provided on the [SPN Portal](https://spn.snowflake.com/s/welcome).

