#!/usr/bin/env python
import RAKE
import operator
import plac

# subtitles ="AI Platform Pipelines has two major parts: (1) the infrastructure for deploying and running structured AI workflows that are integrated with Google Cloud Platform services and (2) the pipeline tools for building, debugging, and sharing pipelines and components. The service runs on a Google Kubernetes cluster that’s automatically created as a part of the installation process, and it’s accessible via the Cloud AI Platform dashboard. With AI Platform Pipelines, developers specify a pipeline using the Kubeflow Pipelines software development kit (SDK), or by customizing the TensorFlow Extended (TFX) Pipeline template with the TFX SDK. This SDK compiles the pipeline and submits it to the Pipelines REST API server, which stores and schedules the pipeline for execution."
s = "professional m******** campaigns with our b*** ***** email software : Advance Mass Sender B*** ***** ********* when it's done right can boost your sales remind clients of products and services and maintain brand visibility Advance Mass Sender is reliable rapid and simple to used with dedicated drag-and-drop software easy-to-manage contact groups and detailed reporting on send p********** leaving you f*** to take care of your sending your m*** ***** messages today by importing your existing email using an email marketing tool that is designed to send mass emails you can design elegant on-brand emails and schedule message delivery in advanced As well as scheduling your campaigns it automatically cleans your data and manages your opt-outs, subscribes and duplicates so you don't have to. Not only does this save you time it makes the entire process more efficient for marketing Mass Sender is arguably one of the best email marketing tools on the market right now Advance Mass Sender is a completely unique and innovative bulk email sender that makes sending mass mailing a breeze It's the first and one of newsletter software that allows you to send unlimited emails with unlimited number of recipients You can send text and himl emails along with attachments templates and multiple connections Try f** **** or buy it now (no recurring fees the fastest email software for personal or business Mass Sender Mass Sender deliver 1*** of your mass email messages to recipient boxes and not to their S*** Mass Sender automatically removes all bad email a******** by processing bounce subscribe and subscribe feature will add and remove people 100% Mass Sender sends bulk email text & himl editor built-in himl Editor"

def Sort_Tuple(tup):
    tup.sort(key = lambda x: x[1])
    return tup


def main():
    stop_dir = "Classification/stopwords.txt"
    rake_object = RAKE.Rake(stop_dir)
    keywords = Sort_Tuple(rake_object.run(s))[-10:]
    print(keywords)


if __name__ == "__main__":
    plac.call(main)
    # get_quarantine()

