# Astro Enhancement Proposals	 
 
The purpose of an Astro Enhancement Proposal (AEP) is to introduce any significant change to the Astro library.	 

This is required in order to balance the need to support new features, use cases, while avoiding accidentally introducing half thought-out interfaces that cause unnecessary problems when changed.	 

This documentation is heavily based on the [Airflow Improvement Proposals](https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+Improvements+Proposals).	 


## What is considered a significant change that needs an API?	 

Any of the following should be considered a major change:	  
* Any change that impacts the public interfaces of the project	 

All the following are considered public interfaces of Astro:	  
* Operators	 


## Lifecycle

*tl;dr;*  
Examples of life cycles:	 

*  Draft -> Proposed -> Accepted -> Completed
*  Draft -> Proposed -> Abandoned


### Draft

Most proposals start with an idea. If you have an idea of how Astro could improve, create a [Github Issue](https://github.com/astro-projects/astro/issues/new) and, label it with `enhancement`.	 

We encourage you to start a Draft document in the [aep](.) folder an make a pull request, so others can discuss it. Please, follow the [AEP template](./AEP-template.md).	 


### Proposed

Once you or someone else feels like there’s a rough consensus on the idea and there’s no strong opposition, you can move your proposal to the Proposed phase, when others can vote on it.

This is done by encouraging people to add the reactions "+1" or "-1" in the Github issue.


### Accepted 

Congratulations! Your proposal has been accepted. 	 

Next up is breaking down tasks using the [Github Issues page](https://github.com/astro-projects/astro/issues/new).	 

Ask a commiter to create a label for your AEP, and link all your issues with it for easier maintainability.	 

We encourage you to involve the Astro community in reviewing the proposed tasks before they are implemented. **HOW?**	 

Once the issues are reviewed, we encourage you following the **Contributing guidelines**.	 


### Completed

Please, move your AEP to `complete` when you consider the main bulk of work has been merged to the `main` branch of the repository. It is okay to leave open issues for minor follow up tasks like adding additional capabilities or similar.	 


### Abandoned	 

AEPs may be moved to abandoned in the following two cases:
 * There has been no owner or intention of developing it further for over a year
 * The owner decides to abandon it. 
 
 The latter can happen if the creator chooses to refactor the AEP into a new AEP for example or if there is a proposal that supersedes the original AEP.

 
## AEPs	 

### Proposed 	 

| ID         | Title                     | Github Issue                                       |	   
| ---------- | ------------------------- | -------------------------------------------------- |	 
| AEP-01     | Support API retrieval     | https://github.com/astro-projects/astro/issues/20  |

